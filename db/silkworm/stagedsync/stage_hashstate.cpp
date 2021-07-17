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

#include <boost/endian/conversion.hpp>

#include <silkworm/common/log.hpp>
#include <silkworm/db/access_layer.hpp>
#include <silkworm/db/stages.hpp>
#include <silkworm/db/tables.hpp>
#include <silkworm/db/util.hpp>
#include <silkworm/etl/collector.hpp>

#include "stagedsync.hpp"

namespace silkworm::stagedsync {

namespace fs = std::filesystem;

/*
 *  Convert get tables configuration pair for incremental promotion
 *  First configuration of the pair is the source and second configuration is the table to fill.
 */
std::pair<db::MapConfig, db::MapConfig> get_tables_for_promote(HashstateOperation operation) {
    switch (operation) {
        case HashstateOperation::HashAccount:
            return {db::table::kPlainAccountChangeSet, db::table::kHashedAccounts};
        case HashstateOperation::HashStorage:
            return {db::table::kPlainStorageChangeSet, db::table::kHashedStorage};
        case HashstateOperation::Code:
            return {db::table::kPlainAccountChangeSet, db::table::kContractCode};
        default:
            std::string error{magic_enum::enum_name<HashstateOperation>(operation)};
            error.append(": unknown operation");
            throw std::runtime_error(error);
    }
}
/*
 *  If we havent done hashstate before(first sync), it is possible to just hash values from plainstates,
 *  This is way faster than using changeset because it uses less database reads.
 */
void hashstate_promote_clean_state(mdbx::txn& txn, std::string etl_path) {
    SILKWORM_LOG(LogLevel::Info) << "Hashing state" << std::endl;

    fs::create_directories(etl_path);
    etl::Collector collector_account(etl_path.c_str(), 512 * kMebi);
    etl::Collector collector_storage(etl_path.c_str(), 512 * kMebi);

    auto src{db::open_cursor(txn, db::table::kPlainState)};
    auto data{src.to_first(/*throw_notfound*/ false)};
    int percent{0};
    uint8_t next_start_byte{0};

    while (data) {
        // TODO (Giulio) -- a byte >= uint64 ??
        if (data.key.at(0) >= next_start_byte) {
            SILKWORM_LOG(LogLevel::Info) << "Progress: " << percent << "%" << std::endl;
            percent += 10;
            next_start_byte += 25;
        }

        // Account
        if (data.key.length() == kAddressLength) {
            etl::Entry entry{Bytes(keccak256(db::from_slice(data.key)).bytes, kHashLength),
                             Bytes(static_cast<uint8_t*>(data.value.iov_base), data.value.length())};
            collector_account.collect(entry);
        } else {
            Bytes new_key(kHashLength * 2 + db::kIncarnationLength, '\0');
            uint32_t new_key_pos{0};

            std::memcpy(&new_key[new_key_pos], keccak256(db::from_slice(data.key).substr(0, kAddressLength)).bytes,
                        kHashLength);
            data.key.remove_prefix(kAddressLength);
            new_key_pos += kAddressLength;

            std::memcpy(&new_key[new_key_pos], data.key.iov_base, db::kIncarnationLength);
            data.key.remove_prefix(db::kIncarnationLength);
            new_key_pos += db::kIncarnationLength;

            std::memcpy(&new_key[new_key_pos], keccak256(db::from_slice(data.key)).bytes, kHashLength);
            etl::Entry entry{new_key, Bytes(static_cast<uint8_t*>(data.value.iov_base), data.value.iov_len)};
            collector_storage.collect(entry);
        }

        data = src.to_next(/*throw_notfound*/ false);
    }

    SILKWORM_LOG(LogLevel::Info) << "Started Account Loading" << std::endl;
    auto target{db::open_cursor(txn, db::table::kHashedAccounts)};
    collector_account.load(target, nullptr, MDBX_put_flags_t::MDBX_APPEND, 10);

    SILKWORM_LOG(LogLevel::Info) << "Started Storage Loading" << std::endl;
    target = db::open_cursor(txn, db::table::kHashedStorage);
    collector_storage.load(target, nullptr, MDBX_put_flags_t::MDBX_APPEND, 10);
}

void hashstate_promote_clean_code(mdbx::txn& txn, std::string etl_path) {
    SILKWORM_LOG(LogLevel::Info) << "Hashing code keys" << std::endl;

    fs::create_directories(etl_path);
    etl::Collector collector(etl_path.c_str(), 512 * kMebi);

    auto tbl{db::open_cursor(txn, db::table::kPlainContractCode)};
    auto data{tbl.to_first(/*throw_notfound*/ false)};
    while (data) {
        Bytes new_key(kHashLength + db::kIncarnationLength, '\0');
        std::memcpy(&new_key[0], keccak256(db::from_slice(data.key.safe_middle(0, kAddressLength))).bytes, kHashLength);
        std::memcpy(&new_key[kHashLength], data.key.safe_middle(kAddressLength, db::kIncarnationLength).iov_base,
                    db::kIncarnationLength);
        etl::Entry entry{new_key, Bytes(static_cast<uint8_t*>(data.value.iov_base), data.value.iov_len)};
        collector.collect(entry);
        data = tbl.to_next(/*throw_notfound*/ false);
    }
    tbl.close();

    SILKWORM_LOG(LogLevel::Info) << "Started Code Loading" << std::endl;
    tbl = db::open_cursor(txn, db::table::kContractCode);
    collector.load(tbl, nullptr, MDBX_put_flags_t::MDBX_APPEND, 10);
}
/*
 *  If we have done hashstate before(not first sync),
 *  We need to use changeset because we can use the progress system.
 *  Note: Standard Promotion is way slower than Clean Promotion
 */
void hashstate_promote(mdbx::txn& txn, HashstateOperation operation) {
    auto [changeset_config, target_config] = get_tables_for_promote(operation);

    auto changeset_table{db::open_cursor(txn, changeset_config)};
    auto plainstate_table{db::open_cursor(txn, db::table::kPlainState)};
    auto codehash_table{db::open_cursor(txn, db::table::kPlainContractCode)};
    auto target_table{db::open_cursor(txn, target_config)};

    auto start_block_number{db::stages::get_stage_progress(txn, db::stages::kHashStateKey) + 1};

    Bytes start_key{db::block_key(start_block_number)};
    auto changeset_data{changeset_table.lower_bound(db::to_slice(start_key), /*throw_notfound*/ false)};

    while (changeset_data) {
        Bytes mdb_key_as_bytes{db::from_slice(changeset_data.key)};
        Bytes mdb_value_as_bytes{db::from_slice(changeset_data.value)};
        auto [db_key, _]{convert_to_db_format(mdb_key_as_bytes, mdb_value_as_bytes)};

        if (operation == HashstateOperation::HashAccount) {
            // We get account and hash its key.
            auto plainstate_data{plainstate_table.find(db::to_slice(db_key), /*throw_notfound*/ false)};
            if (!plainstate_data) {
                changeset_data = changeset_table.to_next(false);
                continue;
            }
            // Hashing
            auto hash{keccak256(db_key)};
            target_table.upsert(db::to_slice(hash.bytes), plainstate_data.value);
            changeset_data = changeset_table.to_next(false);

        } else if (operation == HashstateOperation::HashStorage) {
            // We get storage value and hash its key.
            Bytes key(kHashLength * 2 + db::kIncarnationLength, '\0');
            auto plainstate_data{plainstate_table.find(db::to_slice(db_key), /*throw_notfound*/ false)};
            if (!plainstate_data) {
                changeset_data = changeset_table.to_next(false);
                continue;
            }

            // Hashing
            std::memcpy(&key[0], keccak256(db_key.substr(0, kAddressLength)).bytes, kHashLength);
            std::memcpy(&key[kHashLength], &db_key[kAddressLength], db::kIncarnationLength);
            std::memcpy(&key[kHashLength + db::kIncarnationLength],
                        keccak256(db_key.substr(kAddressLength + db::kIncarnationLength)).bytes, kHashLength);

            target_table.upsert(db::to_slice(key), plainstate_data.value);
            changeset_data = changeset_table.to_next(false);

        } else {
            // get incarnation
            auto encoded_account{plainstate_table.find(db::to_slice(db_key), false)};
            if (!encoded_account) {
                changeset_data = changeset_table.to_next(false);
                continue;
            }
            auto [incarnation, err]{extract_incarnation(db::from_slice(encoded_account.value))};
            rlp::err_handler(err);
            if (incarnation == 0) {
                changeset_data = changeset_table.to_next(false);
                continue;
            }

            // get code hash
            Bytes plain_key(kAddressLength + db::kIncarnationLength, '\0');
            std::memcpy(&plain_key[0], &db_key[0], kAddressLength);
            boost::endian::store_big_u64(&plain_key[kAddressLength], incarnation);
            auto code_hash{codehash_table.find(db::to_slice(plain_key), false)};
            if (!code_hash) {
                changeset_data = changeset_table.to_next(false);
                continue;
            }

            // Hash and concatenate everything together
            Bytes key(kHashLength + db::kIncarnationLength, '\0');
            std::memcpy(&key[0], keccak256(plain_key.substr(0, kAddressLength)).bytes, kHashLength);
            std::memcpy(&key[kHashLength], &plain_key[kAddressLength], db::kIncarnationLength);
            target_table.upsert(db::to_slice(key), code_hash.value);
            changeset_data = changeset_table.to_next(false);
        }
    }
}

StageResult stage_hashstate(db::EnvConfig db_config) {
    fs::path datadir(db_config.path);
    fs::path etl_path(datadir.parent_path() / fs::path("etl-temp"));

    auto env{db::open_env(db_config)};
    auto txn{env.start_write()};

    SILKWORM_LOG(LogLevel::Info) << "Starting HashState" << std::endl;

    auto last_processed_block_number{db::stages::get_stage_progress(txn, db::stages::kHashStateKey)};
    if (last_processed_block_number != 0) {
        SILKWORM_LOG(LogLevel::Info) << "Starting Account Hashing" << std::endl;
        hashstate_promote(txn, HashstateOperation::HashAccount);
        SILKWORM_LOG(LogLevel::Info) << "Starting Storage Hashing" << std::endl;
        hashstate_promote(txn, HashstateOperation::HashStorage);
        SILKWORM_LOG(LogLevel::Info) << "Hashing Code Keys" << std::endl;
        hashstate_promote(txn, HashstateOperation::Code);
    } else {
        hashstate_promote_clean_state(txn, etl_path.string());
        hashstate_promote_clean_code(txn, etl_path.string());
    }
    // Update progress height with last processed block
    db::stages::set_stage_progress(txn, db::stages::kHashStateKey,
                                   db::stages::get_stage_progress(txn, db::stages::kExecutionKey));
    txn.commit();

    SILKWORM_LOG(LogLevel::Info) << "All Done!" << std::endl;
    return StageResult::kStageSuccess;
}

/*
 *  If we have done hashstate before(not first sync),
 *  We need to use changeset because we can use the progress system.
 *  Note: Standard Promotion is way slower than Clean Promotion
 */
void hashstate_unwind(mdbx::txn& txn, uint64_t unwind_to, HashstateOperation operation) {

    auto [changeset_config, target_config] = get_tables_for_promote(operation);

    auto changeset_table{db::open_cursor(txn, changeset_config)};
    auto plainstate_table{db::open_cursor(txn, db::table::kPlainState)};
    auto target_table{db::open_cursor(txn, target_config)};

    Bytes start_key{db::block_key(unwind_to + 1)};
    auto changeset_data{changeset_table.lower_bound(db::to_slice(start_key), /*throw_notfound*/ false)};
    if (!changeset_data) {
        return;
    }

    db::WalkFunc unwind_func;
    switch (operation) {
        case silkworm::stagedsync::HashstateOperation::HashAccount:
            unwind_func = [&target_table](::mdbx::cursor::move_result data) -> bool {
                auto [db_key, _]{convert_to_db_format(db::from_slice(data.key), db::from_slice(data.value))};
                auto hash{keccak256(db_key)};
                if (target_table.seek(db::to_slice(hash.bytes))) {
                    target_table.erase();
                }
                return true;
            };
            break;
        case silkworm::stagedsync::HashstateOperation::HashStorage:
            unwind_func = [&target_table](::mdbx::cursor::move_result data) -> bool {
                auto [db_key, _]{convert_to_db_format(db::from_slice(data.key), db::from_slice(data.value))};

                // We get storage value and hash its key.
                Bytes key(kHashLength * 2 + db::kIncarnationLength, '\0');
                // Hashing
                std::memcpy(&key[0], keccak256(db_key.substr(0, kAddressLength)).bytes, kHashLength);
                std::memcpy(&key[kHashLength], &db_key[kAddressLength], db::kIncarnationLength);
                std::memcpy(&key[kHashLength + db::kIncarnationLength],
                            keccak256(db_key.substr(kAddressLength + db::kIncarnationLength)).bytes, kHashLength);
                if (target_table.seek(db::to_slice(key))) {
                    target_table.erase();
                }
                return true;
            };
            break;
        case silkworm::stagedsync::HashstateOperation::Code:
            unwind_func = [&target_table, &plainstate_table](::mdbx::cursor::move_result data) -> bool {
                auto [db_key, _]{convert_to_db_format(db::from_slice(data.key), db::from_slice(data.value))};
                // Get incarnation
                auto encoded_account{plainstate_table.find(db::to_slice(db_key), false)};
                if (encoded_account) {
                    auto [incarnation, err]{extract_incarnation(db::from_slice(encoded_account.value))};
                    rlp::err_handler(err);
                    if (incarnation) {
                        Bytes key(kHashLength + db::kIncarnationLength, '\0');
                        std::memcpy(&key[0], keccak256(db_key.substr(0, kAddressLength)).bytes, kHashLength);
                        boost::endian::store_big_u64(&key[kHashLength], incarnation);
                        if (target_table.seek(db::to_slice(key))) {
                            target_table.erase();
                        }
                    }
                }
                return true;
            };
            break;
        default:
            std::string error{magic_enum::enum_name<HashstateOperation>(operation)};
            error.append(": unknown operation");
            throw std::runtime_error(error);
    }

    (void)db::for_each(changeset_table, unwind_func);

}

StageResult unwind_hashstate(db::EnvConfig db_config, uint64_t unwind_to) {
    try {
        auto env{db::open_env(db_config)};
        auto txn{env.start_write()};

        auto stage_height{db::stages::get_stage_progress(txn, db::stages::kHashStateKey)};
        if (unwind_to >= stage_height) {
            SILKWORM_LOG(LogLevel::Error)
                << "Stage progress is " << stage_height << " which is <= than requested unwind_to" << std::endl;
            return StageResult::kStageAborted;
        }

        SILKWORM_LOG(LogLevel::Info) << "Unwinding HashState from " << stage_height << " to " << unwind_to << " ..."
                                     << std::endl;

        SILKWORM_LOG(LogLevel::Info) << "[1/3] Hashed accounts ... " << std::endl;
        hashstate_unwind(txn, unwind_to, HashstateOperation::HashAccount);

        SILKWORM_LOG(LogLevel::Info) << "[2/3] Hashed storage ... " << std::endl;
        hashstate_unwind(txn, unwind_to, HashstateOperation::HashStorage);

        SILKWORM_LOG(LogLevel::Info) << "[3/3] Code ... " << std::endl;
        hashstate_unwind(txn, unwind_to, HashstateOperation::Code);

        // Update progress height with last processed block
        db::stages::set_stage_progress(txn, db::stages::kHashStateKey, unwind_to);

        SILKWORM_LOG(LogLevel::Info) << "Committing ... " << std::endl;
        txn.commit();

        SILKWORM_LOG(LogLevel::Info) << "All Done!" << std::endl;
        return StageResult::kStageSuccess;

    } catch (const std::exception& ex) {
        SILKWORM_LOG(LogLevel::Error) << "Unexpected error : " << ex.what() << std::endl;
        return StageResult::kStageAborted;
    }
}
}  // namespace silkworm::stagedsync
