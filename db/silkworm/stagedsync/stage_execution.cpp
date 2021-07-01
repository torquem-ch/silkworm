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

#include "stagedsync.hpp"

#include <string>
#include <filesystem>

#include <silkworm/chain/config.hpp>
#include <silkworm/common/log.hpp>
#include <silkworm/common/magic_enum.hpp>
#include <silkworm/db/access_layer.hpp>
#include <silkworm/db/buffer.hpp>
#include <silkworm/execution/execution.hpp>
#include <silkworm/db/stages.hpp>
#include <silkworm/etl/collector.hpp>
#include <boost/endian/conversion.hpp>

namespace silkworm::stagedsync {

StageResult execute(mdbx::txn& txn, ChainConfig config, uint64_t max_block, uint64_t* block_num, bool write_receipts) {
    db::Buffer buffer{txn};
    AnalysisCache analysis_cache;
    ExecutionStatePool state_pool;
    
    for (; *block_num <= max_block; ++*block_num) {
        std::optional<BlockWithHash> bh{db::read_block(txn, *block_num, /*read_senders=*/true)};
        if (!bh) {
            return StageResult::kStageBadChainSequence;
        }

        auto [receipts, err]{execute_block(bh->block, buffer, config, &analysis_cache, &state_pool)};
        if (err != ValidationResult::kOk) {
            throw std::runtime_error("Validation error " + std::to_string(static_cast<int>(err)) + " at block " + std::to_string(*block_num));
        }

        if (write_receipts) {
            buffer.insert_receipts(*block_num, receipts);
        }

        if (*block_num % 1000 == 0) {
            SILKWORM_LOG(LogLevel::Info) << "Blocks <= " << block_num << " executed" << std::endl;
        }

        if (buffer.current_batch_size() >= kBatchSize) {
            buffer.write_to_db();
            return StageResult::kStageSuccess;
        }
    };

    buffer.write_to_db();
    return StageResult::kStageSuccess;
}

StageResult stage_execution(db::EnvConfig db_config) {

    auto env{db::open_env(db_config)};
    auto txn{env.start_read()};
    auto config{db::read_chain_config(txn)};

    uint64_t max_block{db::stages::get_stage_progress(txn, db::stages::kBlockBodiesKey)};
    uint64_t block_num{db::stages::get_stage_progress(txn, db::stages::kExecutionKey)};
    bool write_receipts{db::read_storage_mode_receipts(txn)};

    if (write_receipts && (!db::migration_happened(txn, "receipts_cbor_encode") ||
                            !db::migration_happened(txn, "receipts_store_logs_separately"))) {
        throw std::runtime_error("Legacy stored receipts are not supported");
    }

    // https://github.com/ledgerwatch/erigon/pull/1342
    if (!db::migration_happened(txn, "acc_change_set_dup_sort_18") ||
        !db::migration_happened(txn, "storage_change_set_dup_sort_22")) {
        throw std::runtime_error("Legacy change sets are not supported");
    }

    // https://github.com/ledgerwatch/erigon/pull/1358
    if (!db::migration_happened(txn, "tx_table_4")) {
        throw std::runtime_error("Legacy stored transactions are not supported\n");
    }

    while (block_num <= max_block) {
        auto execution_code{execute(txn, *config, max_block, &block_num, write_receipts)};
        if (execution_code != StageResult::kStageSuccess) {
            return execution_code;
        }
    };

    return StageResult::kStageSuccess;
}

void collect_for_unwind(Bytes key, Bytes value, lmdb::Table& plain_state_table, lmdb::Table& plain_code_table) {
    if (key.size() == kAddressLength) {
        if (value.size() > 0) {
            auto [account, err]{decode_account_from_storage(value)};
            rlp::err_handler(err);
            if (account.incarnation > 0 && account.code_hash == kEmptyHash) {
                Bytes code_hash_key(kAddressLength + db::kIncarnationLength, '\0');
                std::memcpy(&code_hash_key[0], &key[0], kAddressLength);
                boost::endian::store_big_u64(&code_hash_key[kAddressLength], account.incarnation);
                auto new_code_hash{*plain_code_table.get(code_hash_key)};
                std::memcpy(&account.code_hash.bytes[0], &new_code_hash[0], kHashLength);
            
            }
            // cleaning up contract codes
            auto state_account_encoded{plain_state_table.get(key)};
            if (state_account_encoded != std::nullopt) {
                auto [state_incarnation, err]{extract_incarnation(*state_account_encoded)};
                rlp::err_handler(err);
                // cleanup each code incarnation
                for (uint64_t i = state_incarnation; i > account.incarnation && i > 0; --i) {
                    Bytes key_hash(kAddressLength + 8,'\0');
                    std::memcpy(&key_hash[0], key.data(), kAddressLength);
                    boost::endian::store_big_u64(&key_hash[kAddressLength], i);
                    plain_code_table.del(key_hash);
                }
            }
            auto new_encoded_account{account.encode_for_storage(false)};
            plain_state_table.del(key);
            plain_state_table.put(key, new_encoded_account);
        } else {
            plain_state_table.del(key);
        }
        return;
    }
    auto location{key.substr(kAddressLength + db::kIncarnationLength)};
    plain_state_table.del(key.substr(0, kAddressLength + db::kIncarnationLength), location);
    if (value.size() > 0) {
        auto data{location.append(value)};
        plain_state_table.put(key.substr(0, kAddressLength + db::kIncarnationLength), data);
    }
    return;
}

void walk_collect(lmdb::Table& source, lmdb::Table& plain_state_table, lmdb::Table& plain_code_table, uint64_t unwind_to) {
    MDB_val mdb_key;
    MDB_val mdb_data;
    int rc{source.get_last(&mdb_key, &mdb_data)};
    uint64_t block_number{0};
    while (rc == MDB_SUCCESS) {
        Bytes key(db::from_mdb_val(mdb_key));
        Bytes value(db::from_mdb_val(mdb_data));

        block_number = boost::endian::load_big_u64(&key[0]);
        if (block_number == unwind_to) break;
        auto [new_key, new_value]{convert_to_db_format(key, value)};
        collect_for_unwind(new_key, new_value, plain_state_table, plain_code_table);
        rc = source.get_prev(&mdb_key, &mdb_data);
    }
    if (rc != MDB_NOTFOUND) {
        lmdb::err_handler(rc);
    }
}

void unwind_table_from(lmdb::Table& table, Bytes& starting_key) {
    MDB_val mdb_key{db::to_mdb_val(starting_key)};
    MDB_val mdb_data;

    int rc{table.seek(&mdb_key, &mdb_data)}; 
    while (rc == MDB_SUCCESS) {
        lmdb::err_handler(table.del_current());
        rc = table.get_next(&mdb_key, &mdb_data);
    }
    if (rc != MDB_NOTFOUND) {
        lmdb::err_handler(rc);
    }
}

StageResult unwind_execution(lmdb::DatabaseConfig db_config, uint64_t unwind_to) {
    std::shared_ptr<lmdb::Environment> env{lmdb::get_env(db_config)};
    std::unique_ptr<lmdb::Transaction> txn{env->begin_rw_transaction()};
    // Compute etl temporary path
    fs::path datadir(db_config.path);
    uint64_t block_number{db::stages::get_stage_progress(*txn, db::stages::kExecutionKey)};


    auto plain_state_table{txn->open(db::table::kPlainState, MDB_CREATE)};
    auto plain_code_table{txn->open(db::table::kPlainContractCode, MDB_CREATE)};
    auto account_changeset_table{txn->open(db::table::kPlainAccountChangeSet, MDB_CREATE)};
    auto storage_changeset_table{txn->open(db::table::kPlainStorageChangeSet, MDB_CREATE)};
    auto receipts_table{txn->open(db::table::kBlockReceipts, MDB_CREATE)};
    auto log_table{txn->open(db::table::kLogs, MDB_CREATE)};
    auto traces_table{txn->open(db::table::kCallTraceSet, MDB_CREATE)};

    if (unwind_to == 0) {
        plain_state_table->clear();
        plain_code_table->clear();
        account_changeset_table->clear();
        storage_changeset_table->clear();
        receipts_table->clear();
        log_table->clear();
        traces_table->clear();
        db::stages::set_stage_progress(*txn, db::stages::kExecutionKey, 0);
        lmdb::err_handler(txn->commit());
        return StageResult::kStageSuccess;
    }

    if (unwind_to >= block_number) {
        return StageResult::kStageSuccess;
    }

    SILKWORM_LOG(LogLevel::Info) << "Unwind Execution from " << block_number << " to " << unwind_to << std::endl;

    walk_collect(*account_changeset_table, *plain_state_table, *plain_code_table, unwind_to);
    walk_collect(*storage_changeset_table, *plain_state_table, *plain_code_table, unwind_to);
    // We set the cursor data
    Bytes unwind_to_bytes(8, '\0');
    boost::endian::store_big_u64(&unwind_to_bytes[0], unwind_to+1);

    // Truncate Tables
    unwind_table_from(*account_changeset_table, unwind_to_bytes);
    unwind_table_from(*storage_changeset_table, unwind_to_bytes);
    unwind_table_from(*receipts_table, unwind_to_bytes);
    unwind_table_from(*log_table, unwind_to_bytes);
    unwind_table_from(*traces_table, unwind_to_bytes);
    
    db::stages::set_stage_progress(*txn, db::stages::kExecutionKey, unwind_to);
    lmdb::err_handler(txn->commit());

    return StageResult::kStageSuccess;
}
}