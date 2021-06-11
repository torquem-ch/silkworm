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

#include "stagedsync.hpp"
#include <silkworm/stagedsync/recovery/recovery_farm.hpp>
namespace silkworm::stagedsync {

namespace fs = std::filesystem;

StageResult stage_senders(lmdb::DatabaseConfig db_config) {
    fs::path datadir(db_config.path);
    // Compute etl temporary path
    fs::path etl_path(datadir.parent_path() / fs::path("etl-temp"));
    fs::create_directories(etl_path);
    etl::Collector collector(etl_path.string().c_str(), /* flush size */ 512 * kMebi);

    // Open db and transaction
    auto lmdb_env{lmdb::get_env(db_config)};
    auto lmdb_txn{lmdb_env->begin_rw_transaction()};

    // Create farm instance and do work
    recovery::RecoveryFarm farm(*lmdb_txn, std::thread::hardware_concurrency(), kBatchSize, collector);
    
    auto block_from{db::stages::get_stage_progress(*lmdb_txn, db::stages::kSendersKey)};
    auto block_to{db::stages::get_stage_progress(*lmdb_txn, db::stages::kHeadersKey)};

    return farm.recover(block_from, block_to);
}

StageResult unwind_senders(lmdb::DatabaseConfig db_config, uint64_t unwind_to) {
    fs::path datadir(db_config.path);
    // Compute etl temporary path
    fs::path etl_path(datadir.parent_path() / fs::path("etl-temp"));
    fs::create_directories(etl_path);
    etl::Collector collector(etl_path.string().c_str(), /* flush size */ 512 * kMebi);

    // Open db and transaction
    auto lmdb_env{lmdb::get_env(db_config)};
    auto lmdb_txn{lmdb_env->begin_rw_transaction()};

    // Create farm instance and do work
    recovery::RecoveryFarm farm(*lmdb_txn, std::thread::hardware_concurrency(), kBatchSize, collector);

    return farm.unwind(unwind_to);
}

}
