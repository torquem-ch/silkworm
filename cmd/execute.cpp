/*
   Copyright 2020-2021 The Silkworm Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include <filesystem>

#include <CLI/CLI.hpp>
#include <boost/endian/conversion.hpp>

#include <silkworm/common/log.hpp>
#include <silkworm/common/magic_enum.hpp>
#include <silkworm/db/access_layer.hpp>
#include <silkworm/db/stages.hpp>
#include <silkworm/db/tables.hpp>
#include <silkworm/execution/execution.hpp>
#include <silkworm_tg_api.h>

int main(int argc, char* argv[]) {
    using namespace silkworm;

    CLI::App app{"Execute Ethereum blocks and write the result into the DB"};

    std::string db_path{db::default_path()};
    app.add_option("--chaindata", db_path, "Path to a database populated by Erigon", true)
        ->check(CLI::ExistingDirectory);

    std::string map_size_str{};
    CLI::Option* map_size_option{app.add_option("--lmdb.mapSize", map_size_str, "Lmdb map size")};

    uint64_t to_block{UINT64_MAX};
    app.add_option("--to", to_block, "Block execute up to");

    std::string batch_size_str{"512MB"};
    app.add_option("--batch", batch_size_str, "Batch size of DB changes to accumulate before committing", true);

    CLI11_PARSE(app, argc, argv);

    namespace fs = std::filesystem;

    // Check data.mdb exists in provided directory
    fs::path db_file{fs::path(db_path) / fs::path("data.mdb")};
    if (!fs::exists(db_file)) {
        SILKWORM_LOG(LogLevel::Error) << "Can't find a valid Erigon data file in " << db_path << std::endl;
        return -1;
    }

    // Check provided map size is valid
    auto map_size{parse_size(map_size_str)};
    if (!map_size.has_value()) {
        SILKWORM_LOG(LogLevel::Error) << "Invalid --lmdb.mapSize value provided : " << map_size_str << std::endl;
        return -2;
    }

    auto batch_size{parse_size(batch_size_str)};
    if (!batch_size.has_value()) {
        SILKWORM_LOG(LogLevel::Error) << "Invalid --batch value provided : " << batch_size_str << std::endl;
        return -3;
    }

    SILKWORM_LOG(LogLevel::Info) << "Starting block execution. DB: " << db_file << std::endl;

    try {
        lmdb::DatabaseConfig db_config{db_path};
        if (*map_size_option) {
            db_config.map_size = *map_size;
        }
        db_config.set_readonly(false);
        std::shared_ptr<lmdb::Environment> env{lmdb::get_env(db_config)};
        std::unique_ptr<lmdb::Transaction> txn{env->begin_rw_transaction()};

        bool write_receipts{db::read_storage_mode_receipts(*txn)};
        auto chain_config{db::read_chain_config(*txn)};
        if (!chain_config.has_value()) {
            throw std::runtime_error("Unable to retrieve chain config");
        }

        uint64_t previous_progress{db::stages::get_stage_progress(*txn, db::stages::kExecutionKey)};
        uint64_t current_progress{previous_progress};

        for (uint64_t block_number{previous_progress + 1}; block_number <= to_block; ++block_number) {
            auto [code, lmdb_error_code, current_progress] =
                silkworm_execute_blocks(*txn->handle(), chain_config->chain_id, block_number, to_block, *batch_size,
                                        write_receipts);
            if (code != SilkwormStatusCode::kSilkwormSuccess && code != SilkwormStatusCode::kSilkwormBlockNotFound) {
                SILKWORM_LOG(LogLevel::Error) << "Error in silkworm_execute_blocks: " << magic_enum::enum_name(code)
                                              << ", LMDB: " << lmdb_error_code << std::endl;
                return magic_enum::enum_integer(code);
            }

            block_number = current_progress;

            db::stages::set_stage_progress(*txn, db::stages::kExecutionKey, current_progress);
            lmdb::err_handler(txn->commit());
            txn.reset();

            if (code == SilkwormStatusCode::kSilkwormBlockNotFound) {
                break;
            }

            SILKWORM_LOG(LogLevel::Info) << "Blocks <= " << current_progress << " committed" << std::endl;
            txn = env->begin_rw_transaction();
        }

        if (current_progress > previous_progress) {
            SILKWORM_LOG(LogLevel::Info) << "All blocks <= " << current_progress << " executed and committed"
                                         << std::endl;
        } else {
            SILKWORM_LOG(LogLevel::Warn) << "Nothing to execute" << std::endl;
        }

    } catch (const std::exception& ex) {
        SILKWORM_LOG(LogLevel::Error) << ex.what() << std::endl;
        return -5;
    }

    return 0;
}
