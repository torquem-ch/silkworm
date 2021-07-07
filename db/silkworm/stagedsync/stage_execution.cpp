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
#include <string>

#include <boost/endian/conversion.hpp>

#include <silkworm/chain/config.hpp>
#include <silkworm/common/log.hpp>
#include <silkworm/common/magic_enum.hpp>
#include <silkworm/db/access_layer.hpp>
#include <silkworm/db/buffer.hpp>
#include <silkworm/db/stages.hpp>
#include <silkworm/etl/collector.hpp>
#include <silkworm/execution/execution.hpp>

#include "stagedsync.hpp"

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
            throw std::runtime_error("Validation error " + std::to_string(static_cast<int>(err)) + " at block " +
                                     std::to_string(*block_num));
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

void collect_for_unwind(Bytes key, Bytes value, mdbx::cursor& plain_state_table, mdbx::cursor& plain_code_table) {
    if (key.size() == kAddressLength) {
        if (value.size() > 0) {
            auto [account, err]{decode_account_from_storage(value)};
            rlp::err_handler(err);
            if (account.incarnation > 0 && account.code_hash == kEmptyHash) {
                Bytes code_hash_key(kAddressLength + db::kIncarnationLength, '\0');
                std::memcpy(&code_hash_key[0], &key[0], kAddressLength);
                boost::endian::store_big_u64(&code_hash_key[kAddressLength], account.incarnation);
                auto new_code_hash{plain_code_table.find(db::to_slice(code_hash_key))};
                std::memcpy(&account.code_hash.bytes[0], new_code_hash.value.byte_ptr(), kHashLength);
            }
            // cleaning up contract codes
            auto state_account_encoded{plain_state_table.find(db::to_slice(key), /*throw_notfound=*/false)};
            if (state_account_encoded) {
                auto [state_incarnation, err]{extract_incarnation(db::from_slice(state_account_encoded.value))};
                rlp::err_handler(err);
                // cleanup each code incarnation
                for (uint64_t i = state_incarnation; i > account.incarnation && i > 0; --i) {
                    Bytes key_hash(kAddressLength + 8, '\0');
                    std::memcpy(&key_hash[0], key.data(), kAddressLength);
                    boost::endian::store_big_u64(&key_hash[kAddressLength], i);
                    if (plain_code_table.seek(db::to_slice(key_hash))) {
                        plain_code_table.erase();
                    }
                }
            }
            auto new_encoded_account{account.encode_for_storage(false)};
            if (plain_state_table.seek(db::to_slice(key))) {
                plain_state_table.erase(/*whole_multivalue*/ true);
            }
            plain_state_table.upsert(db::to_slice(key), db::to_slice(new_encoded_account));
        } else {
            if (plain_code_table.seek(db::to_slice(key))) {
                plain_code_table.erase();
            }
        }
        return;
    }
    auto location{key.substr(kAddressLength + db::kIncarnationLength)};
    auto key1{key.substr(0, kAddressLength + db::kIncarnationLength)};
    if (plain_state_table.find_multivalue(db::to_slice(key1), db::to_slice(location))) {
        plain_state_table.erase();
    }
    if (value.size() > 0) {
        auto data{location.append(value)};
        plain_state_table.upsert(db::to_slice(key1), db::to_slice(data));
    }
    return;
}

void walk_collect(mdbx::cursor& source, mdbx::cursor& plain_state_table, mdbx::cursor& plain_code_table,
                  uint64_t unwind_to) {
    uint64_t block_number{0};
    auto src_data{source.to_last(/*throw_notfound*/ false)};
    while (src_data) {
        Bytes key(db::from_slice(src_data.key));
        Bytes value(db::from_slice(src_data.value));
        block_number = boost::endian::load_big_u64(&key[0]);
        if (block_number == unwind_to) {
            break;
        }
        auto [new_key, new_value]{convert_to_db_format(key, value)};
        collect_for_unwind(new_key, new_value, plain_state_table, plain_code_table);
        src_data = source.to_previous(/*throw_notfound*/ false);
    }
}

void unwind_table_from(mdbx::cursor& table, Bytes& starting_key) {
    if (table.lower_bound(db::to_slice(starting_key))) {
        table.erase();
        while (table.to_next(/*throw_notfound*/ false)) {
            table.erase();
        }
    }
}

StageResult unwind_execution(db::EnvConfig db_config, uint64_t unwind_to) {
    auto env{db::open_env(db_config)};
    auto txn{env.start_write()};
    uint64_t block_number{db::stages::get_stage_progress(txn, db::stages::kExecutionKey)};

    auto plain_state_table{db::open_cursor(txn, db::table::kPlainState)};
    auto plain_code_table{db::open_cursor(txn, db::table::kPlainContractCode)};
    auto account_changeset_table{db::open_cursor(txn, db::table::kPlainAccountChangeSet)};
    auto storage_changeset_table{db::open_cursor(txn, db::table::kPlainStorageChangeSet)};
    auto receipts_table{db::open_cursor(txn, db::table::kBlockReceipts)};
    auto log_table{db::open_cursor(txn, db::table::kLogs)};
    auto traces_table{db::open_cursor(txn, db::table::kCallTraceSet)};

    if (unwind_to == 0) {
        txn.clear_map(plain_state_table.map());
        txn.clear_map(plain_code_table.map());
        txn.clear_map(account_changeset_table.map());
        txn.clear_map(storage_changeset_table.map());
        txn.clear_map(receipts_table.map());
        txn.clear_map(log_table.map());
        txn.clear_map(traces_table.map());
        db::stages::set_stage_progress(txn, db::stages::kExecutionKey, 0);
        txn.commit();
        return StageResult::kStageSuccess;
    }

    if (unwind_to >= block_number) {
        return StageResult::kStageSuccess;
    }

    SILKWORM_LOG(LogLevel::Info) << "Unwind Execution from " << block_number << " to " << unwind_to << std::endl;

    walk_collect(account_changeset_table, plain_state_table, plain_code_table, unwind_to);
    walk_collect(storage_changeset_table, plain_state_table, plain_code_table, unwind_to);
    // We set the cursor data
    Bytes unwind_to_bytes(8, '\0');
    boost::endian::store_big_u64(&unwind_to_bytes[0], unwind_to + 1);

    // Truncate Tables
    unwind_table_from(account_changeset_table, unwind_to_bytes);
    unwind_table_from(storage_changeset_table, unwind_to_bytes);
    unwind_table_from(receipts_table, unwind_to_bytes);
    unwind_table_from(log_table, unwind_to_bytes);
    unwind_table_from(traces_table, unwind_to_bytes);

    db::stages::set_stage_progress(txn, db::stages::kExecutionKey, unwind_to);
    txn.commit();

    return StageResult::kStageSuccess;
}
}  // namespace silkworm::stagedsync
