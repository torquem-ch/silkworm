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
#include <iomanip>
#include <iostream>
#include <string>
#include <unordered_map>

#include <boost/endian/conversion.hpp>

#include <silkworm/common/cast.hpp>
#include <silkworm/common/log.hpp>
#include <silkworm/db/access_layer.hpp>
#include <silkworm/db/bitmap.hpp>
#include <silkworm/db/stages.hpp>
#include <silkworm/db/tables.hpp>
#include <silkworm/etl/collector.hpp>

namespace silkworm::stagedsync {

constexpr size_t kBitmapBufferSizeLimit = 256 * kMebi;

namespace fs = std::filesystem;

StageResult history_index_stage(lmdb::DatabaseConfig db_config, bool storage) {
    fs::path datadir(db_config.path);
    fs::path etl_path(datadir.parent_path() / fs::path("etl-temp"));
    fs::create_directories(etl_path);
    etl::Collector collector(etl_path.string().c_str(), /* flush size */ 512 * kMebi);

    std::shared_ptr<lmdb::Environment> env{lmdb::get_env(db_config)};
    std::unique_ptr<lmdb::Transaction> txn{env->begin_rw_transaction()};
    // We take data from header table and transform it and put it in blockhashes table
    lmdb::TableConfig changeset_config =
        storage ? db::table::kPlainStorageChangeSet : db::table::kPlainAccountChangeSet;
    lmdb::TableConfig index_config = storage ? db::table::kStorageHistory : db::table::kAccountHistory;
    const char *stage_key = storage ? db::stages::kStorageHistoryIndexKey : db::stages::kAccountHistoryKey;
    auto changeset_table{txn->open(changeset_config)};
    std::unordered_map<std::string, roaring::Roaring64Map> bitmaps;

    auto last_processed_block_number{db::stages::get_stage_progress(*txn, stage_key)};

    // Extract
    Bytes start(8, '\0');
    boost::endian::store_big_u64(&start[0], last_processed_block_number);
    MDB_val mdb_key{db::to_mdb_val(start)};
    MDB_val mdb_data;

    SILKWORM_LOG(LogLevel::Info) << "Started " << (storage ? "Storage" : "Account") << " Index Extraction"
                                    << std::endl;

    size_t allocated_space{0};
    uint64_t block_number{0};
    int rc{changeset_table->seek(&mdb_key, &mdb_data)};  // Sets cursor to nearest key greater equal than this
    while (!rc) {                                        /* Loop as long as we have no errors*/
        std::string composite_key;
        if (storage) {
            char composite_key_array[kHashLength + kAddressLength];
            std::memcpy(&composite_key_array[0], &static_cast<uint8_t *>(mdb_key.mv_data)[8], kAddressLength);
            std::memcpy(&composite_key_array[kAddressLength], mdb_data.mv_data, kHashLength);
            composite_key = std::string(composite_key_array);
        } else {
            composite_key = std::string(static_cast<char *>(mdb_data.mv_data), kAddressLength);
        }

        if (bitmaps.find(composite_key) == bitmaps.end()) {
            bitmaps.emplace(composite_key, roaring::Roaring64Map());
        }
        block_number = boost::endian::load_big_u64(static_cast<uint8_t *>(mdb_key.mv_data));
        bitmaps.at(composite_key).add(block_number);
        allocated_space += 8;
        if (64 * bitmaps.size() + allocated_space > kBitmapBufferSizeLimit) {
            for (const auto &[key, bm] : bitmaps) {
                Bytes bitmap_bytes(bm.getSizeInBytes(), '\0');
                bm.write(byte_ptr_cast(bitmap_bytes.data()));
                etl::Entry entry{Bytes(byte_ptr_cast(key.c_str()), key.size()), bitmap_bytes};
                collector.collect(entry);
            }
            SILKWORM_LOG(LogLevel::Info) << "Current Block: " << block_number << std::endl;
            bitmaps.clear();
            allocated_space = 0;
        }
        rc = changeset_table->get_next(&mdb_key, &mdb_data);
    }

    if (rc && rc != MDB_NOTFOUND) { /* MDB_NOTFOUND is not actually an error rather eof */
        lmdb::err_handler(rc);
    }

    for (const auto &[key, bm] : bitmaps) {
        Bytes bitmap_bytes(bm.getSizeInBytes(), '\0');
        bm.write(byte_ptr_cast(bitmap_bytes.data()));
        etl::Entry entry{Bytes(byte_ptr_cast(key.c_str()), key.size()), bitmap_bytes};
        collector.collect(entry);
    }
    bitmaps.clear();

    SILKWORM_LOG(LogLevel::Info) << "Latest Block: " << block_number << std::endl;
    // Proceed only if we've done something
    if (collector.size()) {
        SILKWORM_LOG(LogLevel::Info) << "Started Loading" << std::endl;

        unsigned int db_flags{last_processed_block_number ? 0u : MDB_APPEND};

        // Eventually load collected items WITH transform (may throw)
        collector.load(
            txn->open(index_config, MDB_CREATE).get(),
            [](etl::Entry entry, lmdb::Table *history_index_table, unsigned int db_flags) {
                auto bm{roaring::Roaring64Map::readSafe(byte_ptr_cast(entry.value.data()), entry.value.size())};
                Bytes last_chunk_index(entry.key.size() + 8, '\0');
                std::memcpy(&last_chunk_index[0], &entry.key[0], entry.key.size());
                boost::endian::store_big_u64(&last_chunk_index[entry.key.size()], UINT64_MAX);
                auto previous_bitmap_bytes{history_index_table->get(last_chunk_index)};
                if (previous_bitmap_bytes.has_value()) {
                    bm |= roaring::Roaring64Map::readSafe(byte_ptr_cast(previous_bitmap_bytes->data()),
                                                            previous_bitmap_bytes->size());
                    db_flags = 0;
                }
                while (bm.cardinality() > 0) {
                    auto current_chunk{db::bitmap::cut_left(bm, db::bitmap::kBitmapChunkLimit)};
                    // make chunk index
                    Bytes chunk_index(entry.key.size() + 8, '\0');
                    std::memcpy(&chunk_index[0], &entry.key[0], entry.key.size());
                    uint64_t suffix{bm.cardinality() == 0 ? UINT64_MAX : current_chunk.maximum()};
                    boost::endian::store_big_u64(&chunk_index[entry.key.size()], suffix);
                    Bytes current_chunk_bytes(current_chunk.getSizeInBytes(), '\0');
                    current_chunk.write(byte_ptr_cast(&current_chunk_bytes[0]));
                    history_index_table->put(chunk_index, current_chunk_bytes, db_flags);
                }
            },
            db_flags, /* log_every_percent = */ 20);

        // Update progress height with last processed block
        db::stages::set_stage_progress(*txn, stage_key, block_number);
        lmdb::err_handler(txn->commit());

    } else {
        SILKWORM_LOG(LogLevel::Info) << "Nothing to process" << std::endl;
    }

    SILKWORM_LOG(LogLevel::Info) << "All Done" << std::endl;

    return StageResult::kStageSuccess;
}

void truncate_indexes(lmdb::Table* index_table, Bytes prefix, uint64_t unwind_to) {
    Bytes prev_key{};
    Bytes prev_bitmap{};
    // End suffix is 0xffffffffffffffff
    Bytes end_suffix(8, '\0');
    boost::endian::store_big_u64(&end_suffix[0], UINT64_MAX);
    MDB_val mdb_key{db::to_mdb_val(prefix)};
    MDB_val mdb_data;
    int rc{index_table->seek(&mdb_key, &mdb_data)};  // Sets cursor to nearest key greater equal than this
    while (!rc) {                                        /* Loop as long as we have no errors*/
        Bytes key{db::from_mdb_val(mdb_key)};
        Bytes bitmap_bytes{db::from_mdb_val(mdb_key)};

        uint64_t maximum{boost::endian::load_big_u64(&key[key.size() - 8])};
        if (key.substr(0, key.size() - 8).compare(prefix) != 0) {
            return;
        }
        if (unwind_to > maximum) {
            // unwind point not yet reached
            rc = index_table->get_next(&mdb_key, &mdb_data);
            prev_key = key;
            prev_bitmap = bitmap_bytes;
            continue;
        }
        auto bm{roaring::Roaring64Map::readSafe(byte_ptr_cast(bitmap_bytes.data()), bitmap_bytes.size())};
        bm &= roaring::Roaring64Map(roaring::api::roaring_bitmap_from_range(0, unwind_to, 1));
        if (bm.cardinality() == 0) {
            // delete all of the keys with the suffix
            while(!rc) {
                lmdb::err_handler(index_table->del_current());
                rc = index_table->get_next(&mdb_key, &mdb_data);
                auto next_key{db::from_mdb_val(mdb_key)};
                if (next_key.substr(next_key.size() - 8).compare(end_suffix) != 0) {
                    break;
                }
            }
            lmdb::err_handler(rc);
            if (prev_key.size() > 0) {
                index_table->del(prev_key);
                std::memcpy(&prev_key[prev_key.size() - 8], &end_suffix[0], 8);
                index_table->put(prev_key, prev_bitmap);
            }
            return;
        } else {
            // delete all of the keys with the suffix
            while(!rc) {
                lmdb::err_handler(index_table->del_current());
                rc = index_table->get_next(&mdb_key, &mdb_data);
                auto next_key{db::from_mdb_val(mdb_key)};
                if (next_key.substr(0, next_key.size() - 8).compare(prefix) != 0) {
                    break;
                }
            }
            lmdb::err_handler(rc);

            Bytes new_bitmap_bytes(bm.getSizeInBytes(), '\0');
            bm.write(byte_ptr_cast(&new_bitmap_bytes[0]));
            std::memcpy(&key[key.size() - 8], &end_suffix[0], 8);
            index_table->put(key, new_bitmap_bytes);
            
            return;
        }
    }
    lmdb::err_handler(rc);
}

StageResult history_index_unwind(lmdb::DatabaseConfig db_config, uint64_t unwind_to, bool storage) {
    fs::path datadir(db_config.path);
    fs::path etl_path(datadir.parent_path() / fs::path("etl-temp"));
    fs::create_directories(etl_path);
    etl::Collector collector(etl_path.string().c_str(), /* flush size */ 512 * kMebi);

    std::shared_ptr<lmdb::Environment> env{lmdb::get_env(db_config)};
    std::unique_ptr<lmdb::Transaction> txn{env->begin_rw_transaction()};
    // We take data from header table and transform it and put it in blockhashes table
    lmdb::TableConfig changeset_config =
        storage ? db::table::kPlainStorageChangeSet : db::table::kPlainAccountChangeSet;
    lmdb::TableConfig index_config = storage ? db::table::kStorageHistory : db::table::kAccountHistory;
    const char *stage_key = storage ? db::stages::kStorageHistoryIndexKey : db::stages::kAccountHistoryKey;
    auto changeset_table{txn->open(changeset_config)};
    auto index_table{txn->open(index_config)};
    std::unordered_map<std::string, roaring::Roaring64Map> bitmaps;

    if (unwind_to > db::stages::get_stage_progress(*txn, stage_key)) {
        return StageResult::kStageSuccess;
    }

    // Extract
    Bytes start(8, '\0');
    boost::endian::store_big_u64(&start[0], unwind_to + 1);
    MDB_val mdb_key{db::to_mdb_val(start)};
    MDB_val mdb_data;

    SILKWORM_LOG(LogLevel::Info) << "Started " << (storage ? "Storage" : "Account") << " Index Unwind " << "to block number: "
                                    << unwind_to << std::endl;

    int rc{changeset_table->seek(&mdb_key, &mdb_data)};  // Sets cursor to nearest key greater equal than this
    while (!rc) {                                        /* Loop as long as we have no errors*/
        if (storage) {
            Bytes composite_key(kHashLength + kAddressLength, '\0');
            std::memcpy(&composite_key[0], &static_cast<uint8_t *>(mdb_key.mv_data)[8], kAddressLength);
            std::memcpy(&composite_key[kAddressLength], mdb_data.mv_data, kHashLength);
            truncate_indexes(index_table.get(), composite_key, unwind_to);
        } else {
            Bytes composite_key(kAddressLength, '\0');
            std::memcpy(&composite_key[0], &static_cast<uint8_t *>(mdb_data.mv_data)[0], kAddressLength);
            truncate_indexes(index_table.get(), composite_key, unwind_to);
        }
    }

    if (rc && rc != MDB_NOTFOUND) { /* MDB_NOTFOUND is not actually an error rather eof */
        lmdb::err_handler(rc);
    }

    db::stages::set_stage_progress(*txn, stage_key, unwind_to);
    lmdb::err_handler(txn->commit());

    SILKWORM_LOG(LogLevel::Info) << "All Done" << std::endl;

    return StageResult::kStageSuccess;
}

StageResult stage_account_history(lmdb::DatabaseConfig db_config) { return history_index_stage(db_config, false); }
StageResult stage_storage_history(lmdb::DatabaseConfig db_config) { return history_index_stage(db_config, true); }

StageResult unwind_account_history(lmdb::DatabaseConfig db_config, uint64_t unwind_to) { return history_index_unwind(db_config, unwind_to, false); }

StageResult unwind_storage_history(lmdb::DatabaseConfig db_config, uint64_t unwind_to) { return history_index_unwind(db_config, unwind_to, true); }
}