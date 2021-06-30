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

#include <filesystem>
#include <iomanip>
#include <iostream>
#include <string>
#include <unordered_map>

#include <CLI/CLI.hpp>
#include <boost/endian/conversion.hpp>

#include <silkworm/stagedsync/stagedsync.hpp>
#include <silkworm/db/stages.hpp>
#include <silkworm/common/log.hpp>

using namespace silkworm;

int main(int argc, char *argv[]) {
    namespace fs = std::filesystem;

    CLI::App app{"Generates History Indexes"};

    std::string db_path{db::default_path()};
    bool full{false}, storage{false};
    app.add_option("--chaindata", db_path, "Path to a database populated by Erigon", true)
        ->check(CLI::ExistingDirectory);

    app.add_flag("--full", full, "Start making history indexes from block 0");
    app.add_flag("--storage", storage, "Do history of storages");

    CLI11_PARSE(app, argc, argv);

    // Check data.mdb exists in provided directory
    fs::path db_file{fs::path(db_path) / fs::path("mdbx.dat")};
    if (!fs::exists(db_file)) {
        SILKWORM_LOG(LogLevel::Error) << "Can't find a valid Erigon data file in " << db_path << std::endl;
        return -1;
    }

    db::EnvConfig db_config{db_path};
    db_config.set_readonly(false);

    db::MapConfig index_config = storage ? db::table::kStorageHistory : db::table::kAccountHistory;
    const char *stage_key = storage ? db::stages::kStorageHistoryIndexKey : db::stages::kAccountHistoryKey;

    try {
        if (full) {
            auto env{db::open_env(db_config)};
            auto txn{env.start_write()};
            txn.clear_map(db::open_map(txn, index_config));
            txn.commit();
        }
        
        if (storage) {
            stagedsync::check_stagedsync_error(stagedsync::stage_storage_history(db_config));
        } else {
            stagedsync::check_stagedsync_error(stagedsync::stage_account_history(db_config)); 
        }

    } catch (const std::exception &ex) {
        SILKWORM_LOG(LogLevel::Error) << ex.what() << std::endl;
        return -5;
    }
    return 0;
}
