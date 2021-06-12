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

#include <filesystem>
#include <iostream>

#include <boost/endian/conversion.hpp>

#include <silkworm/common/log.hpp>
#include <silkworm/common/magic_enum.hpp>
#include <silkworm/db/access_layer.hpp>
#include <silkworm/db/stages.hpp>
#include <silkworm/db/tables.hpp>
#include <silkworm/db/util.hpp>
#include <silkworm/etl/collector.hpp>

namespace silkworm::stagedsync {

namespace fs = std::filesystem;

/*
 *  Convert get tables configuration pair for incremental promotion
 *  First configuration of the pair is the source and second configuration is the table to fill.
 */
std::pair<lmdb::TableConfig, lmdb::TableConfig> get_tables_for_promote(HashstateOperation operation) {
    switch (operation) {
        case HashstateOperation::HashAccount:
            return {db::table::kPlainAccountChangeSet, db::table::kHashedAccounts};
        case HashstateOperation::HashStorage:
            return {db::table::kPlainStorageChangeSet, db::table::kHashedStorage};
        default:
            return {db::table::kPlainAccountChangeSet, db::table::kContractCode};
    }
}
/*
 *  If we havent done hashstate before(first sync), it is possible to just hash values from plainstates,
 *  This is way faster than using changeset because it uses less database reads.
 */
void hashstate_promote_clean_state(lmdb::Transaction* txn, std::string etl_path) {
    SILKWORM_LOG(LogLevel::Info) << "Hashing state" << std::endl;
    auto source_table{txn->open(db::table::kPlainState)};
    MDB_val mdb_key;
    MDB_val mdb_data;
    int rc{source_table->get_first(&mdb_key, &mdb_data)};
    fs::create_directories(etl_path);
    etl::Collector collector_account(etl_path.c_str(), 512 * kMebi);
    etl::Collector collector_storage(etl_path.c_str(), 512 * kMebi);
    int percent{0};
    uint64_t next_start_byte{0};
    while (!rc) { /* Loop as long as we have no errors*/
        Bytes mdb_key_as_bytes{db::from_mdb_val(mdb_key)};
        Bytes mdb_value_as_bytes{db::from_mdb_val(mdb_data)};
        if (mdb_key_as_bytes.at(0) >= next_start_byte) {
            SILKWORM_LOG(LogLevel::Info) << "Progress: " << percent << "%" << std::endl;
            percent += 10;
            next_start_byte += 25;
        }
        // Account
        if (mdb_key.mv_size == kAddressLength) {
            etl::Entry entry{Bytes(keccak256(mdb_key_as_bytes).bytes, kHashLength), mdb_value_as_bytes};
            collector_account.collect(entry);
        } else {
            Bytes new_key(kHashLength * 2 + db::kIncarnationLength, '\0');
            std::memcpy(&new_key[0], keccak256(mdb_key_as_bytes.substr(0, kAddressLength)).bytes, kHashLength);
            std::memcpy(&new_key[kHashLength], &mdb_key_as_bytes[kAddressLength], db::kIncarnationLength);
            std::memcpy(&new_key[kHashLength + db::kIncarnationLength],
                        keccak256(mdb_key_as_bytes.substr(kAddressLength + db::kIncarnationLength)).bytes, kHashLength);
            etl::Entry entry{new_key, mdb_value_as_bytes};
            collector_storage.collect(entry);
        }
        rc = source_table->get_next(&mdb_key, &mdb_data);
    }

    if (rc && rc != MDB_NOTFOUND) { /* MDB_NOTFOUND is not actually an error rather eof */
        lmdb::err_handler(rc);
    }

    SILKWORM_LOG(LogLevel::Info) << "Started Account Loading" << std::endl;
    collector_account.load(txn->open(db::table::kHashedAccounts, MDB_CREATE).get(), nullptr, MDB_APPEND,
                           /* log_every_percent = */ 10);

    SILKWORM_LOG(LogLevel::Info) << "Started Storage Loading" << std::endl;
    collector_storage.load(txn->open(db::table::kHashedStorage, MDB_CREATE).get(), nullptr, MDB_APPENDDUP,
                           /* log_every_percent = */ 10);
}

void hashstate_promote_clean_code(lmdb::Transaction* txn, std::string etl_path) {
    auto source_table{txn->open(db::table::kPlainContractCode)};
    MDB_val mdb_key;
    MDB_val mdb_data;
    int rc{source_table->get_first(&mdb_key, &mdb_data)};
    fs::create_directories(etl_path);
    etl::Collector collector(etl_path.c_str(), 512 * kMebi);
    SILKWORM_LOG(LogLevel::Info) << "Hashing code keys" << std::endl;
    while (!rc) { /* Loop as long as we have no errors*/
        Bytes mdb_key_as_bytes{db::from_mdb_val(mdb_key)};
        Bytes mdb_value_as_bytes{db::from_mdb_val(mdb_data)};

        Bytes new_key(kHashLength + db::kIncarnationLength, '\0');
        std::memcpy(&new_key[0], keccak256(mdb_key_as_bytes.substr(0, kAddressLength)).bytes, kHashLength);
        std::memcpy(&new_key[kHashLength], &mdb_key_as_bytes[kAddressLength], db::kIncarnationLength);
        etl::Entry entry{new_key, mdb_value_as_bytes};
        collector.collect(entry);
        rc = source_table->get_next(&mdb_key, &mdb_data);
    }
    if (rc != MDB_NOTFOUND) {
        lmdb::err_handler(rc);
    }
    SILKWORM_LOG(LogLevel::Info) << "Started Code Loading" << std::endl;
    collector.load(txn->open(db::table::kContractCode, MDB_CREATE).get(), nullptr, MDB_APPEND,
                   /* log_every_percent = */ 10);
}
/*
 *  If we have done hashstate before(not first sync),
 *  We need to use changeset because we can use the progress system.
 *  Note: Standard Promotion is way slower than Clean Promotion
 */
void hashstate_promote(lmdb::Transaction* txn, HashstateOperation operation) {
    auto [changeset_config, target_config] = get_tables_for_promote(operation);
    auto changeset_table{txn->open(changeset_config)};
    auto plainstate_table{txn->open(db::table::kPlainState)};
    auto codehash_table{txn->open(db::table::kPlainContractCode)};
    auto target_table{txn->open(target_config)};
    auto start_block_number{db::stages::get_stage_progress(*txn, db::stages::kHashStateKey) + 1};
    Bytes start_key(8, '\0');
    boost::endian::store_big_u64(&start_key[0], start_block_number);
    MDB_val mdb_key{db::to_mdb_val(start_key)};
    MDB_val mdb_data;
    int rc{changeset_table->seek(&mdb_key, &mdb_data)};

    while (!rc) {
        Bytes mdb_key_as_bytes{db::from_mdb_val(mdb_key)};
        Bytes mdb_value_as_bytes{db::from_mdb_val(mdb_data)};
        auto [db_key, _]{convert_to_db_format(mdb_key_as_bytes, mdb_value_as_bytes)};
        if (operation == HashstateOperation::HashAccount) {
            // We get account and hash its key.
            auto value{plainstate_table->get(db_key)};
            if (value == std::nullopt) {
                rc = changeset_table->get_next(&mdb_key, &mdb_data);
                continue;
            }
            // Hashing
            auto hash{keccak256(db_key)};
            target_table->put(full_view(hash.bytes), *value, 0);
            rc = changeset_table->get_next(&mdb_key, &mdb_data);
        } else if (operation == HashstateOperation::HashStorage) {
            // We get storage value and hash its key.
            Bytes key(kHashLength * 2 + db::kIncarnationLength, '\0');
            auto value{plainstate_table->get(db_key)};
            if (value == std::nullopt) {
                rc = changeset_table->get_next(&mdb_key, &mdb_data);
                continue;
            }
            // Hashing
            std::memcpy(&key[0], keccak256(db_key.substr(0, kAddressLength)).bytes, kHashLength);
            std::memcpy(&key[kHashLength], &db_key[kAddressLength], db::kIncarnationLength);
            std::memcpy(&key[kHashLength + db::kIncarnationLength],
                        keccak256(db_key.substr(kAddressLength + db::kIncarnationLength)).bytes, kHashLength);
            target_table->put(key, *value, 0);
            rc = changeset_table->get_next(&mdb_key, &mdb_data);
        } else {
            // get incarnation
            auto encoded_account{plainstate_table->get(db_key)};
            if (encoded_account == std::nullopt) {
                rc = changeset_table->get_next(&mdb_key, &mdb_data);
                continue;
            }
            auto [incarnation, err]{extract_incarnation(*encoded_account)};
            rlp::err_handler(err);
            if (incarnation == 0) {
                rc = changeset_table->get_next(&mdb_key, &mdb_data);
                continue;
            }
            // get code hash
            Bytes plain_key(kAddressLength + db::kIncarnationLength, '\0');
            std::memcpy(&plain_key[0], &db_key[0], kAddressLength);
            boost::endian::store_big_u64(&plain_key[kAddressLength], incarnation);
            auto code_hash{codehash_table->get(plain_key)};
            if (code_hash == std::nullopt) {
                rc = changeset_table->get_next(&mdb_key, &mdb_data);
                continue;
            }
            // Hash and concatenate everything together
            Bytes key(kHashLength + db::kIncarnationLength, '\0');
            std::memcpy(&key[0], keccak256(plain_key.substr(0, kAddressLength)).bytes, kHashLength);
            std::memcpy(&key[kHashLength], &plain_key[kAddressLength], db::kIncarnationLength);
            target_table->put(key, *code_hash, 0);
            rc = changeset_table->get_next(&mdb_key, &mdb_data);
        }
    }
}

/*
 *  If we have done hashstate before(not first sync),
 *  We need to use changeset because we can use the progress system.
 *  Note: Standard Promotion is way slower than Clean Promotion
 */
void hashstate_unwind(lmdb::Transaction* txn, HashstateOperation operation, uint64_t unwind_to) {
    auto [changeset_config, target_config] = get_tables_for_promote(operation);
    auto changeset_table{txn->open(changeset_config)};
    auto plainstate_table{txn->open(db::table::kPlainState)};
    auto codehash_table{txn->open(db::table::kPlainContractCode)};
    auto target_table{txn->open(target_config)};
    Bytes start_key(8, '\0');
    boost::endian::store_big_u64(&start_key[0], unwind_to + 1);
    MDB_val mdb_key{db::to_mdb_val(start_key)};
    MDB_val mdb_data;
    int rc{changeset_table->seek(&mdb_key, &mdb_data)};

    while (!rc) {
        Bytes mdb_key_as_bytes{db::from_mdb_val(mdb_key)};
        Bytes mdb_value_as_bytes{db::from_mdb_val(mdb_data)};
        auto [db_key, _]{convert_to_db_format(mdb_key_as_bytes, mdb_value_as_bytes)};
        if (operation == HashstateOperation::HashAccount) {
            // Hashing
            auto hash{keccak256(db_key)};
            target_table->del(full_view(hash.bytes));
            rc = changeset_table->get_next(&mdb_key, &mdb_data);
        } else if (operation == HashstateOperation::HashStorage) {
            Bytes key(kHashLength * 2 + db::kIncarnationLength, '\0');
            // Hashing
            std::memcpy(&key[0], keccak256(db_key.substr(0, kAddressLength)).bytes, kHashLength);
            std::memcpy(&key[kHashLength], &db_key[kAddressLength], db::kIncarnationLength);
            std::memcpy(&key[kHashLength + db::kIncarnationLength],
                        keccak256(db_key.substr(kAddressLength + db::kIncarnationLength)).bytes, kHashLength);
            target_table->del(key);
            rc = changeset_table->get_next(&mdb_key, &mdb_data);
        } else {
            // get incarnation
            auto encoded_account{plainstate_table->get(db_key)};
            if (encoded_account == std::nullopt) {
                rc = changeset_table->get_next(&mdb_key, &mdb_data);
                continue;
            }
            auto [incarnation, err]{extract_incarnation(*encoded_account)};
            rlp::err_handler(err);
            if (incarnation == 0) {
                rc = changeset_table->get_next(&mdb_key, &mdb_data);
                continue;
            }
            // get code hash
            Bytes plain_key(kAddressLength + db::kIncarnationLength, '\0');
            std::memcpy(&plain_key[0], &db_key[0], kAddressLength);
            boost::endian::store_big_u64(&plain_key[kAddressLength], incarnation);
            // Hash and concatenate everything together
            Bytes key(kHashLength + db::kIncarnationLength, '\0');
            std::memcpy(&key[0], keccak256(plain_key.substr(0, kAddressLength)).bytes, kHashLength);
            std::memcpy(&key[kHashLength], &plain_key[kAddressLength], db::kIncarnationLength);
            target_table->del(key);
            rc = changeset_table->get_next(&mdb_key, &mdb_data);
        }
    }
}

StageResult stage_hashstate(lmdb::DatabaseConfig db_config) {
    fs::path datadir(db_config.path);
    fs::path etl_path(datadir.parent_path() / fs::path("etl-temp"));

    std::shared_ptr<lmdb::Environment> env{lmdb::get_env(db_config)};
    std::unique_ptr<lmdb::Transaction> txn{env->begin_rw_transaction()};

    SILKWORM_LOG(LogLevel::Info) << "Starting HashState" << std::endl;

    auto last_processed_block_number{db::stages::get_stage_progress(*txn, db::stages::kHashStateKey)};
    if (last_processed_block_number != 0) {
        SILKWORM_LOG(LogLevel::Info) << "Starting Account Hashing" << std::endl;
        hashstate_promote(txn.get(), HashstateOperation::HashAccount);
        SILKWORM_LOG(LogLevel::Info) << "Starting Storage Hashing" << std::endl;
        hashstate_promote(txn.get(), HashstateOperation::HashStorage);
        SILKWORM_LOG(LogLevel::Info) << "Hashing Code Keys" << std::endl;
        hashstate_promote(txn.get(), HashstateOperation::Code);
    } else {
        hashstate_promote_clean_state(txn.get(), etl_path.string());
        hashstate_promote_clean_code(txn.get(), etl_path.string());
    }
    // Update progress height with last processed block
    db::stages::set_stage_progress(*txn, db::stages::kHashStateKey,
                                    db::stages::get_stage_progress(*txn, db::stages::kExecutionKey));
    lmdb::err_handler(txn->commit());
    txn.reset();
    SILKWORM_LOG(LogLevel::Info) << "All Done!" << std::endl;
    return StageResult::kStageSuccess;
}

StageResult unwind_hashstate(lmdb::DatabaseConfig db_config, uint64_t unwind_to) {
    fs::path datadir(db_config.path);
    fs::path etl_path(datadir.parent_path() / fs::path("etl-temp"));

    std::shared_ptr<lmdb::Environment> env{lmdb::get_env(db_config)};
    std::unique_ptr<lmdb::Transaction> txn{env->begin_rw_transaction()};

    SILKWORM_LOG(LogLevel::Info) << "Unwinding HashState to block number:  " << unwind_to << std::endl;

    if (unwind_to > db::stages::get_stage_progress(*txn, db::stages::kHashStateKey)) {
            return StageResult::kStageSuccess;
    }

    hashstate_unwind(txn.get(), HashstateOperation::HashAccount, unwind_to);
    hashstate_unwind(txn.get(), HashstateOperation::HashStorage, unwind_to);
    hashstate_unwind(txn.get(), HashstateOperation::Code, unwind_to);
    // Update progress height with last processed block
    db::stages::set_stage_progress(*txn, db::stages::kHashStateKey, unwind_to);
    lmdb::err_handler(txn->commit());
    SILKWORM_LOG(LogLevel::Info) << "All Done!" << std::endl;

    return StageResult::kStageSuccess;
}
}