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
#include <iostream>

#include <boost/endian/conversion.hpp>

#include <silkworm/common/log.hpp>
#include <silkworm/db/access_layer.hpp>
#include <silkworm/db/stages.hpp>
#include <silkworm/db/tables.hpp>
#include <silkworm/etl/collector.hpp>

#include "stagedsync.hpp"

namespace silkworm::stagedsync {

namespace fs = std::filesystem;

static Bytes compact(Bytes& b) {
    std::string::size_type offset{b.find_first_not_of(uint8_t{0})};
    if (offset != std::string::npos) {
        return b.substr(offset);
    }
    return b;
}

StageResult stage_tx_lookup(db::EnvConfig db_config) {
    fs::path datadir(db_config.path);
    fs::path etl_path(datadir.parent_path() / fs::path("etl-temp"));
    fs::create_directories(etl_path);
    etl::Collector collector(etl_path.string().c_str(), /* flush size */ 512 * kMebi);

    auto env{db::open_env(db_config)};
    auto txn{env.start_write()};

    auto last_processed_block_number{db::stages::get_stage_progress(txn, db::stages::kTxLookupKey)};
    uint64_t block_number{0};

    // We take data from header table and transform it and put it in blockhashes table
    auto bodies_table{db::open_cursor(txn, db::table::kBlockBodies)};
    auto transactions_table{db::open_cursor(txn, db::table::kEthTx)};

    // Extract
    Bytes start(8, '\0');
    boost::endian::store_big_u64(&start[0], last_processed_block_number + 1);

    SILKWORM_LOG(LogLevel::Info) << "Started Tx Lookup Extraction" << std::endl;

    auto bodies_data{bodies_table.lower_bound(db::to_slice(start), /*throw_notfound*/ false)};
    while (bodies_data) {
        auto body_rlp{db::from_slice(bodies_data.value)};
        auto body{db::detail::decode_stored_block_body(body_rlp)};
        Bytes block_number_as_bytes(static_cast<uint8_t*>(bodies_data.key.iov_base), 8);
        auto lookup_block_data{compact(block_number_as_bytes)};
        block_number = boost::endian::load_big_u64(&block_number_as_bytes[0]);

        if (body.txn_count) {
            Bytes tx_base_id(8, '\0');
            boost::endian::store_big_u64(tx_base_id.data(), body.base_txn_id);
            auto tx_data{transactions_table.lower_bound(db::to_slice(tx_base_id), /*throw_notfound*/ false)};
            uint64_t tx_count{0};

            while (tx_data && tx_count < body.txn_count) {
                auto tx_view{db::from_slice(tx_data.value)};
                auto hash{keccak256(tx_view)};
                etl::Entry entry{Bytes(hash.bytes, 32), Bytes(lookup_block_data.data(), lookup_block_data.size())};
                collector.collect(entry);
                ++tx_count;
                tx_data = transactions_table.to_next(/*throw_notfound*/ false);
            }
        }

        // Save last processed block_number and expect next in sequence
        if (block_number % 100000 == 0) {
            SILKWORM_LOG(LogLevel::Info) << "Tx Lookup Extraction Progress << " << block_number << std::endl;
        }

        bodies_data = bodies_table.to_next(/*throw_notfound*/ false);
    }

    SILKWORM_LOG(LogLevel::Info) << "Entries Collected << " << collector.size() << std::endl;

    // Proceed only if we've done something
    if (collector.size()) {
        SILKWORM_LOG(LogLevel::Info) << "Started tx Hashes Loading" << std::endl;

        /*
         * If we're on first sync then we shouldn't have any records in target
         * table. For this reason we can apply MDB_APPEND to load as
         * collector (with no transform) ensures collected entries
         * are already sorted. If instead target table contains already
         * some data the only option is to load in upsert mode as we
         * cannot guarantee keys are sorted amongst different calls
         * of this stage
         */
        auto target_table{db::open_cursor(txn, db::table::kTxLookup)};
        auto target_table_rcount{txn.get_map_stat(target_table.map()).ms_entries};
        MDBX_put_flags_t db_flags{target_table_rcount ? MDBX_put_flags_t::MDBX_UPSERT : MDBX_put_flags_t::MDBX_APPEND};

        // Eventually load collected items with no transform (may throw)
        collector.load(target_table, nullptr, db_flags, /* log_every_percent = */ 10);

        // Update progress height with last processed block
        db::stages::set_stage_progress(txn, db::stages::kTxLookupKey, block_number);
        txn.commit();

    } else {
        SILKWORM_LOG(LogLevel::Info) << "Nothing to process" << std::endl;
    }

    SILKWORM_LOG(LogLevel::Info) << "All Done" << std::endl;

    return StageResult::kStageSuccess;
}

StageResult unwind_tx_lookup(db::EnvConfig db_config, uint64_t unwind_to) {
    fs::path datadir(db_config.path);

    auto env{db::open_env(db_config)};
    auto txn{env.start_write()};

    if (unwind_to >= db::stages::get_stage_progress(txn, db::stages::kTxLookupKey)) {
        return StageResult::kStageSuccess;
    }
    // We take data from header table and transform it and put it in blockhashes table
    auto bodies_table{db::open_cursor(txn, db::table::kBlockBodies)};
    auto transactions_table{db::open_cursor(txn, db::table::kEthTx)};
    auto lookup_table{db::open_cursor(txn, db::table::kTxLookup)};

    // Extract
    Bytes start(8, '\0');
    boost::endian::store_big_u64(&start[0], unwind_to + 1);

    SILKWORM_LOG(LogLevel::Info) << "Started Tx Lookup Unwind, from: " << db::stages::get_stage_progress(txn, db::stages::kTxLookupKey)
        <<  " to: " << unwind_to << std::endl;

    auto bodies_data{bodies_table.lower_bound(db::to_slice(start), /*throw_notfound*/ false)};
    while (bodies_data) {
        auto body_rlp{db::from_slice(bodies_data.value)};
        auto body{db::detail::decode_stored_block_body(body_rlp)};

        if (body.txn_count) {
            Bytes tx_base_id(8, '\0');
            boost::endian::store_big_u64(tx_base_id.data(), body.base_txn_id);
            auto tx_data{transactions_table.lower_bound(db::to_slice(tx_base_id), /*throw_notfound*/ false)};
            uint64_t tx_count{0};

            while (tx_data && tx_count < body.txn_count) {
                auto tx_view{db::from_slice(tx_data.value)};
                auto hash{keccak256(tx_view)};
                auto data{lookup_table.find(db::to_slice(hash.bytes), false)};
                if (!data) {
                    tx_data = transactions_table.to_next(/*throw_notfound*/ false);
                    ++tx_count;
                    continue;
                }
                lookup_table.erase();
                ++tx_count;
                tx_data = transactions_table.to_next(/*throw_notfound*/ false);
            }
        }

        bodies_data = bodies_table.to_next(/*throw_notfound*/ false);
    }


    SILKWORM_LOG(LogLevel::Info) << "All Done" << std::endl;
    db::stages::set_stage_progress(txn, db::stages::kTxLookupKey, unwind_to);
    txn.commit();
    return StageResult::kStageSuccess;
}

}  // namespace silkworm::stagedsync
