/*
   Copyright 2021 The Silkworm Authors

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

#include <CLI/CLI.hpp>

#include <boost/filesystem.hpp>
#include <silkworm/etl/collector.hpp>
#include <silkworm/common/log.hpp>
#include <silkworm/db/access_layer.hpp>
#include <silkworm/db/util.hpp>
#include <silkworm/crypto/ecdsa.hpp>
#include <silkworm/db/tables.hpp>
#include <silkworm/db/stages.hpp>
#include <silkworm/types/transaction.hpp>
#include <boost/endian/conversion.hpp>
#include <silkworm/common/util.hpp>
#include <silkworm/chain/config.hpp>
#include <iostream>

using namespace silkworm;

int main(int argc, char* argv[]) {
    namespace fs = boost::filesystem;

    CLI::App app{"Check Tx Hashes => BlockNumber mapping in database"};

    std::string db_path{db::default_path()};
    size_t block_from;
    app.add_option("-d,--datadir", db_path, "Path to a database populated by Turbo-Geth", true)
        ->check(CLI::ExistingDirectory);
    app.add_option("--from", block_from, "Initial block number to process (inclusive)", true)
        ->check(CLI::Range(1u, UINT32_MAX));
    CLI11_PARSE(app, argc, argv);

    Logger::default_logger().set_local_timezone(true);  // for compatibility with TG logging

    // Check data.mdb exists in provided directory
    boost::filesystem::path db_file{boost::filesystem::path(db_path) / boost::filesystem::path("data.mdb")};
    if (!boost::filesystem::exists(db_file)) {
        SILKWORM_LOG(LogError) << "Can't find a valid TG data file in " << db_path << std::endl;
        return -1;
    }
    fs::path datadir(db_path);
    fs::path etl_path(datadir.parent_path() / fs::path("etl-temp"));
    fs::create_directories(etl_path);
    etl::Collector collector(etl_path.string().c_str(), /* flush size */ 512 * kMebi);

    lmdb::DatabaseConfig db_config{db_path};
    std::shared_ptr<lmdb::Environment> env{lmdb::get_env(db_config)};
    std::unique_ptr<lmdb::Transaction> txn{env->begin_ro_transaction()};

    auto bodies_table{txn->open(db::table::kBlockBodies)};
    auto tx_lookup_table{txn->open(db::table::kTxLookup)};
    auto transactions_table{txn->open(db::table::kEthTx)};
    uint64_t expected_block_number{0};

    try {

        SILKWORM_LOG(LogInfo) << "Checking Transaction Lookups..." << std::endl;

        MDB_val mdb_key, mdb_data;
        int rc{bodies_table->get_first(&mdb_key, &mdb_data)};

        while (!rc) {
            auto body_rlp{db::from_mdb_val(mdb_data)};
            auto body{db::detail::decode_stored_block_body(body_rlp)};
            if (body.txn_count > 0) {
                Bytes transaction_key(8, '\0');
                boost::endian::store_big_u64(transaction_key.data(), body.base_txn_id);
                MDB_val tx_mdb_key{db::to_mdb_val(transaction_key)};
                MDB_val tx_mdb_data;

                uint64_t i{0};
                for (int rc{transactions_table->seek_exact(&tx_mdb_key, &tx_mdb_data)};
                     rc != MDB_NOTFOUND && i < body.txn_count;
                     rc = transactions_table->get_next(&tx_mdb_key, &tx_mdb_data), ++i) {
                    ByteView tx_rlp{db::from_mdb_val(tx_mdb_data)};
                    auto hash{keccak256(tx_rlp)};
                    auto hash_view{full_view(hash.bytes)};
                    auto actual_block_number{boost::endian::load_big_u64(tx_lookup_table->get(hash_view)->data())};
                    if (actual_block_number != expected_block_number) {
                        SILKWORM_LOG(LogError)
                            << "Mismatch: Expected Number for hash: " << to_hex(hash_view) << " is "
                            << expected_block_number << ", but got: " << actual_block_number << std::endl;
                    }
                }
            }

            if (expected_block_number % 100000 == 0) {
                SILKWORM_LOG(LogInfo) << "Scanned blocks " << expected_block_number << std::endl;
            }

            expected_block_number++;
            rc = bodies_table->get_next(&mdb_key, &mdb_data);
        }

        if (rc != MDB_NOTFOUND) {
            lmdb::err_handler(rc);
        }

        SILKWORM_LOG(LogInfo) << "Check finished" << std::endl;
    } catch (const std::exception& ex) {
        SILKWORM_LOG(LogError) << ex.what() << std::endl;
        return -5;
    }
    return 0;
}