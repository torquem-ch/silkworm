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
#include <silkworm/db/access_layer.hpp>
#include <silkworm/db/stages.hpp>
#include <silkworm/db/tables.hpp>
#include <silkworm/etl/collector.hpp>

namespace silkworm::stagedsync {

namespace fs = std::filesystem;


static Bytes compact(Bytes& b) {
    std::string::size_type offset{b.find_first_not_of(uint8_t{0})};
    if (offset != std::string::npos) {
        return b.substr(offset);
    }
    return b;
}

StageResult stage_tx_lookup(lmdb::DatabaseConfig db_config) {
    fs::path datadir(db_config.path);
    fs::path etl_path(datadir.parent_path() / fs::path("etl-temp"));
    fs::create_directories(etl_path);
    etl::Collector collector(etl_path.string().c_str(), /* flush size */ 512 * kMebi);

    std::shared_ptr<lmdb::Environment> env{lmdb::get_env(db_config)};
    std::unique_ptr<lmdb::Transaction> txn{env->begin_rw_transaction()};
    // We take data from header table and transform it and put it in blockhashes table
    auto bodies_table{txn->open(db::table::kBlockBodies)};
    auto transactions_table{txn->open(db::table::kEthTx)};

    auto last_processed_block_number{db::stages::get_stage_progress(*txn, db::stages::kTxLookupKey)};
    uint64_t block_number{0};

    // Extract
    Bytes start(8, '\0');
    boost::endian::store_big_u64(&start[0], last_processed_block_number + 1);
    MDB_val mdb_key{db::to_mdb_val(start)};
    MDB_val mdb_data;
    SILKWORM_LOG(LogLevel::Info) << "Started Tx Lookup Extraction" << std::endl;
    int rc{bodies_table->seek(&mdb_key, &mdb_data)};  // Sets cursor to nearest key greater equal than this
    while (!rc) {                                     /* Loop as long as we have no errors*/
        auto body_rlp{db::from_mdb_val(mdb_data)};
        auto body{db::detail::decode_stored_block_body(body_rlp)};
        Bytes block_number_as_bytes(static_cast<unsigned char*>(mdb_key.mv_data), 8);
        auto lookup_block_data{compact(block_number_as_bytes)};
        block_number = boost::endian::load_big_u64(&block_number_as_bytes[0]);
        if (body.txn_count > 0) {
            Bytes transaction_key(8, '\0');
            boost::endian::store_big_u64(transaction_key.data(), body.base_txn_id);
            MDB_val tx_key_mdb{db::to_mdb_val(transaction_key)};
            MDB_val tx_data_mdb{};

            uint64_t i{0};
            for (rc = transactions_table->seek_exact(&tx_key_mdb, &tx_data_mdb);
                    rc != MDB_NOTFOUND && i < body.txn_count;
                    rc = transactions_table->get_next(&tx_key_mdb, &tx_data_mdb), ++i) {
                lmdb::err_handler(rc);
                // Take transaction rlp, then hash it in order to get the transaction hash
                ByteView tx_rlp{db::from_mdb_val(tx_data_mdb)};
                auto hash{keccak256(tx_rlp)};
                etl::Entry entry{Bytes(hash.bytes, 32), Bytes(lookup_block_data.data(), lookup_block_data.size())};
                collector.collect(entry);
            }
        }
        // Save last processed block_number and expect next in sequence
        if (block_number % 100000 == 0) {
            SILKWORM_LOG(LogLevel::Info) << "Tx Lookup Extraction Progress << " << block_number << std::endl;
        }
        rc = bodies_table->get_next(&mdb_key, &mdb_data);
    }

    if (rc && rc != MDB_NOTFOUND) { /* MDB_NOTFOUND is not actually an error rather eof */
        lmdb::err_handler(rc);
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
        auto target_table{txn->open(db::table::kTxLookup, MDB_CREATE)};
        size_t target_table_rcount{0};
        lmdb::err_handler(target_table->get_rcount(&target_table_rcount));
        unsigned int db_flags{target_table_rcount ? 0u : MDB_APPEND};

        // Eventually load collected items with no transform (may throw)
        collector.load(target_table.get(), nullptr, db_flags, /* log_every_percent = */ 10);

        // Update progress height with last processed block
        db::stages::set_stage_progress(*txn, db::stages::kTxLookupKey, block_number);
        lmdb::err_handler(txn->commit());

    } else {
        SILKWORM_LOG(LogLevel::Info) << "Nothing to process" << std::endl;
    }

    SILKWORM_LOG(LogLevel::Info) << "All Done" << std::endl;

    return StageResult::kStageSuccess;
}

StageResult unwind_tx_lookup(lmdb::DatabaseConfig db_config, uint64_t unwind_to) {
    std::shared_ptr<lmdb::Environment> env{lmdb::get_env(db_config)};
    std::unique_ptr<lmdb::Transaction> txn{env->begin_rw_transaction()};

    uint64_t block_number{db::stages::get_stage_progress(*txn, db::stages::kTxLookupKey)};
    if (unwind_to >= block_number) {
        return StageResult::kStageSuccess;
    }
    SILKWORM_LOG(LogLevel::Info) << "Unwind Lookups from " << block_number << " to " << unwind_to << std::endl;

    auto bodies_table{txn->open(db::table::kBlockBodies)};
    auto transactions_table{txn->open(db::table::kEthTx)};
    auto tx_lookup_table{txn->open(db::table::kTxLookup)};

    Bytes start(8, '\0');
    boost::endian::store_big_u64(&start[0], unwind_to + 1);
    MDB_val mdb_key{db::to_mdb_val(start)};
    MDB_val mdb_data;
    SILKWORM_LOG(LogLevel::Info) << "Started Tx Lookup Unwinding to block number: " << unwind_to << std::endl;
    int rc{bodies_table->seek(&mdb_key, &mdb_data)};  // Sets cursor to nearest key greater equal than this
    while (!rc) {                                     /* Loop as long as we have no errors*/
        auto body_rlp{db::from_mdb_val(mdb_data)};
        auto body{db::detail::decode_stored_block_body(body_rlp)};
        Bytes block_number_as_bytes(static_cast<unsigned char*>(mdb_key.mv_data), 8);
        block_number = boost::endian::load_big_u64(&block_number_as_bytes[0]);
        if (body.txn_count > 0) {
            Bytes transaction_key(8, '\0');
            boost::endian::store_big_u64(transaction_key.data(), body.base_txn_id);
            MDB_val tx_key_mdb{db::to_mdb_val(transaction_key)};
            MDB_val tx_data_mdb{};

            uint64_t i{0};
            for (rc = transactions_table->seek_exact(&tx_key_mdb, &tx_data_mdb);
                    rc != MDB_NOTFOUND && i < body.txn_count;
                    rc = transactions_table->get_next(&tx_key_mdb, &tx_data_mdb), ++i) {
                lmdb::err_handler(rc);
                // Take transaction rlp, then hash it and delete the entry
                ByteView tx_rlp{db::from_mdb_val(tx_data_mdb)};
                auto hash{keccak256(tx_rlp)};
                tx_lookup_table->del(Bytes(hash.bytes, 32));
            }
        }
        rc = bodies_table->get_next(&mdb_key, &mdb_data);
    }
    if (rc != MDB_NOTFOUND) {
        lmdb::err_handler(rc);
    }
    lmdb::err_handler(txn->commit());
    return StageResult::kStageSuccess;
}
}