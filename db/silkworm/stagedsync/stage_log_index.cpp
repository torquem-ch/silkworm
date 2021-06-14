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
#include <string>
#include <thread>
#include <unordered_map>
#include <boost/endian/conversion.hpp>

#include <silkworm/stagedsync/listener_log_index.hpp>
#include <silkworm/common/cast.hpp>
#include <silkworm/common/log.hpp>
#include <silkworm/db/access_layer.hpp>
#include <silkworm/db/bitmap.hpp>
#include <silkworm/db/stages.hpp>
#include <silkworm/db/tables.hpp>
#include <silkworm/etl/collector.hpp>

namespace silkworm::stagedsync {

namespace fs = std::filesystem;

constexpr size_t kBitmapBufferSizeLimit = 512 * kMebi;

void loader_function(etl::Entry entry, lmdb::Table *target_table, unsigned int db_flags) {
    auto bm{roaring::Roaring::readSafe(byte_ptr_cast(entry.value.data()), entry.value.size())};
    Bytes last_chunk_index(entry.key.size() + 4, '\0');
    std::memcpy(&last_chunk_index[0], &entry.key[0], entry.key.size());
    boost::endian::store_big_u32(&last_chunk_index[entry.key.size()], UINT32_MAX);
    auto previous_bitmap_bytes{target_table->get(last_chunk_index)};
    if (previous_bitmap_bytes.has_value()) {
        bm |= roaring::Roaring::readSafe(byte_ptr_cast(previous_bitmap_bytes->data()), previous_bitmap_bytes->size());
        db_flags = 0;
    }
    while (bm.cardinality() > 0) {
        auto current_chunk{db::bitmap::cut_left(bm, db::bitmap::kBitmapChunkLimit)};
        // make chunk index
        Bytes chunk_index(entry.key.size() + 4, '\0');
        std::memcpy(&chunk_index[0], &entry.key[0], entry.key.size());
        uint64_t suffix{bm.cardinality() == 0 ? UINT32_MAX : current_chunk.maximum()};
        boost::endian::store_big_u32(&chunk_index[entry.key.size()], suffix);
        Bytes current_chunk_bytes(current_chunk.getSizeInBytes(), '\0');
        current_chunk.write(byte_ptr_cast(&current_chunk_bytes[0]));
        target_table->put(chunk_index, current_chunk_bytes, db_flags);
    }
}

void flush_bitmaps(etl::Collector &collector, std::unordered_map<std::string, roaring::Roaring> &map) {
    for (const auto &[key, bm] : map) {
        Bytes bitmap_bytes(bm.getSizeInBytes(), '\0');
        bm.write(byte_ptr_cast(bitmap_bytes.data()));
        etl::Entry entry{Bytes(byte_ptr_cast(key.c_str()), key.size()), bitmap_bytes};
        collector.collect(entry);
    }
    map.clear();
}

StageResult stage_log_index(lmdb::DatabaseConfig db_config) {
    fs::path datadir(db_config.path);
    fs::path etl_path(datadir.parent_path() / fs::path("etl-temp"));
    fs::create_directories(etl_path);
    etl::Collector topic_collector(etl_path.string().c_str(), /* flush size */ 256 * kMebi);
    etl::Collector addresses_collector(etl_path.string().c_str(), /* flush size */ 256 * kMebi);

    std::shared_ptr<lmdb::Environment> env{lmdb::get_env(db_config)};
    std::unique_ptr<lmdb::Transaction> txn{env->begin_rw_transaction()};
    // We take data from header table and transform it and put it in blockhashes table
    auto log_table{txn->open(db::table::kLogs)};

    auto last_processed_block_number{db::stages::get_stage_progress(*txn, db::stages::kLogIndexKey)};
    // Extract
    Bytes start(8, '\0');
    boost::endian::store_big_u64(&start[0], last_processed_block_number);
    MDB_val mdb_key{db::to_mdb_val(start)};
    MDB_val mdb_data;

    SILKWORM_LOG(LogLevel::Info) << "Started Log Index Extraction" << std::endl;

    uint64_t block_number{0};
    uint64_t topics_allocated_space{0};
    uint64_t addrs_allocated_space{0};
    std::unordered_map<std::string, roaring::Roaring> topic_bitmaps;
    std::unordered_map<std::string, roaring::Roaring> addresses_bitmaps;
    listener_log_index current_listener(block_number, &topic_bitmaps, &addresses_bitmaps, &topics_allocated_space,
                                        &addrs_allocated_space);
    int rc{log_table->seek(&mdb_key, &mdb_data)};  // Sets cursor to nearest key greater equal than this
    while (!rc) {                                  /* Loop as long as we have no errors*/
        block_number = boost::endian::load_big_u64(static_cast<uint8_t *>(mdb_key.mv_data));
        current_listener.set_block_number(block_number);
        cbor::input input(static_cast<uint8_t *>(mdb_data.mv_data), mdb_data.mv_size);
        cbor::decoder decoder(input, current_listener);
        decoder.run();
        if (topics_allocated_space > kBitmapBufferSizeLimit) {
            flush_bitmaps(topic_collector, topic_bitmaps);
            SILKWORM_LOG(LogLevel::Info) << "Current Block: " << block_number << std::endl;
            topics_allocated_space = 0;
        }

        if (addrs_allocated_space > kBitmapBufferSizeLimit) {
            flush_bitmaps(addresses_collector, addresses_bitmaps);
            SILKWORM_LOG(LogLevel::Info) << "Current Block: " << block_number << std::endl;
            addrs_allocated_space = 0;
        }

        rc = log_table->get_next(&mdb_key, &mdb_data);
    }

    if (rc && rc != MDB_NOTFOUND) { /* MDB_NOTFOUND is not actually an error rather eof */
        lmdb::err_handler(rc);
    }

    flush_bitmaps(topic_collector, topic_bitmaps);
    flush_bitmaps(addresses_collector, addresses_bitmaps);

    SILKWORM_LOG(LogLevel::Info) << "Latest Block: " << block_number << std::endl;
    // Proceed only if we've done something
    SILKWORM_LOG(LogLevel::Info) << "Started Topics Loading" << std::endl;
    // if stage has never been touched then appending is safe
    unsigned int db_flags{last_processed_block_number ? 0u : MDB_APPEND};

    // Eventually load collected items WITH transform (may throw)
    topic_collector.load(txn->open(db::table::kLogTopicIndex, MDB_CREATE).get(), loader_function, db_flags,
                            /* log_every_percent = */ 10);
    SILKWORM_LOG(LogLevel::Info) << "Started Address Loading" << std::endl;
    addresses_collector.load(txn->open(db::table::kLogAddressIndex, MDB_CREATE).get(), loader_function, db_flags,
                                /* log_every_percent = */ 10);

    // Update progress height with last processed block
    db::stages::set_stage_progress(*txn, db::stages::kLogIndexKey, block_number);
    lmdb::err_handler(txn->commit());
    txn.reset();
    SILKWORM_LOG(LogLevel::Info) << "All Done" << std::endl;

    return StageResult::kStageSuccess;
}

void truncate_indexes(lmdb::Table* index_table, Bytes prefix, uint64_t unwind_to) {
    Bytes prev_key{};
    Bytes prev_bitmap{};
    // End suffix is 0xffffffffffffffff
    Bytes end_suffix(4, '\0');
    boost::endian::store_big_u64(&end_suffix[0], UINT64_MAX);
    MDB_val mdb_key{db::to_mdb_val(prefix)};
    MDB_val mdb_data;
    int rc{index_table->seek(&mdb_key, &mdb_data)};  // Sets cursor to nearest key greater equal than this
    while (!rc) {                                        /* Loop as long as we have no errors*/
        Bytes key{db::from_mdb_val(mdb_key)};
        Bytes bitmap_bytes{db::from_mdb_val(mdb_key)};

        uint64_t maximum{boost::endian::load_big_u32(&key[key.size() - 4])};
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
        auto bm{roaring::Roaring::readSafe(byte_ptr_cast(bitmap_bytes.data()), bitmap_bytes.size())};
        bm &= roaring::Roaring(roaring::api::roaring_bitmap_from_range(0, unwind_to, 1));
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

StageResult unwind_log_index(lmdb::DatabaseConfig db_config, uint64_t unwind_to) {
    std::shared_ptr<lmdb::Environment> env{lmdb::get_env(db_config)};
    std::unique_ptr<lmdb::Transaction> txn{env->begin_rw_transaction()};
    // We take data from header table and transform it and put it in blockhashes table
    auto log_table{txn->open(db::table::kLogs)};
    auto log_addresses_table{txn->open(db::table::kLogAddressIndex, MDB_CREATE)};
    auto log_topics_table{txn->open(db::table::kLogTopicIndex, MDB_CREATE)};
    auto last_processed_block_number{db::stages::get_stage_progress(*txn, db::stages::kLogIndexKey)};
    if (unwind_to > last_processed_block_number) {
        return StageResult::kStageSuccess;
    }
    // Extract
    Bytes start(8, '\0');
    boost::endian::store_big_u64(&start[0], unwind_to+1);
    MDB_val mdb_key{db::to_mdb_val(start)};
    MDB_val mdb_data;

    SILKWORM_LOG(LogLevel::Info) << "Started Log Index Extraction" << std::endl;
    // addrs_allocated_space and topics_allocated_space are only used to initialize the listener
    uint64_t addrs_allocated_space{0};
    uint64_t topics_allocated_space{0};


    std::unordered_map<std::string, roaring::Roaring> topic_bitmaps;
    std::unordered_map<std::string, roaring::Roaring> addresses_bitmaps;

    listener_log_index current_listener(unwind_to, &topic_bitmaps, &addresses_bitmaps, &topics_allocated_space,
                                        &addrs_allocated_space);
    int rc{log_table->seek(&mdb_key, &mdb_data)};  // Sets cursor to nearest key greater equal than this
    while (!rc) {                                  /* Loop as long as we have no errors*/
        current_listener.set_block_number(boost::endian::load_big_u64(static_cast<uint8_t *>(mdb_key.mv_data)));
        cbor::input input(static_cast<uint8_t *>(mdb_data.mv_data), mdb_data.mv_size);
        cbor::decoder decoder(input, current_listener);
        decoder.run();
        for (const auto &[key, _] : topic_bitmaps) {
            truncate_indexes(log_topics_table.get(), Bytes(byte_ptr_cast(key.c_str()), key.size()), unwind_to);
        }
        topic_bitmaps.clear();
        for (const auto &[key, _] : addresses_bitmaps) {
            truncate_indexes(log_addresses_table.get(), Bytes(byte_ptr_cast(key.c_str()), key.size()), unwind_to);
        }
        addresses_bitmaps.clear();

        rc = log_table->get_next(&mdb_key, &mdb_data);
    }

    if (rc && rc != MDB_NOTFOUND) { /* MDB_NOTFOUND is not actually an error rather eof */
        lmdb::err_handler(rc);
    }

    // Update progress height with last processed block
    db::stages::set_stage_progress(*txn, db::stages::kLogIndexKey, unwind_to);
    lmdb::err_handler(txn->commit());
    txn.reset();
    SILKWORM_LOG(LogLevel::Info) << "All Done" << std::endl;

    return StageResult::kStageSuccess;
}

}
