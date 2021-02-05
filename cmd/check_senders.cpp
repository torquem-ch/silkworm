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

#include <CLI/CLI.hpp>
#include <atomic>
#include <boost/endian.hpp>
#include <boost/filesystem.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/signals2.hpp>
#include <csignal>
#include <ethash/keccak.hpp>
#include <iostream>
#include <queue>
#include <silkworm/chain/config.hpp>
#include <silkworm/common/log.hpp>
#include <silkworm/common/worker.hpp>
#include <silkworm/crypto/ecdsa.hpp>
#include <silkworm/db/access_layer.hpp>
#include <silkworm/db/stages.hpp>
#include <silkworm/db/util.hpp>
#include <silkworm/etl/collector.hpp>
#include <silkworm/types/block.hpp>
#include <string>
#include <thread>

namespace fs = boost::filesystem;
using namespace silkworm;

std::atomic_bool should_stop_{false};           // Request for stop from user or OS
std::atomic_bool main_thread_error_{false};     // Error detected in main thread
std::atomic_bool workers_thread_error_{false};  // Error detected in one of workers threads
std::atomic_uint workers_in_flight{0};          // Number of workers in flight

struct app_options_t {
    std::string datadir{};          // Provided database path
    uint64_t mapsize{0};            // Provided lmdb map size
    size_t batch_size{100'000};     // Number of work packages to serve e worker
    uint32_t block_from{1u};        // Initial block number to start from
    uint32_t block_to{UINT32_MAX};  // Final block number to process
    bool replay{false};             // Whether to replay already extracted senders
    bool debug{false};              // Whether to display some debug info
    bool rundry{false};             // Runs in dry mode (no data is persisted on disk)
};

void sig_handler(int signum) {
    (void)signum;
    std::cout << std::endl << " Got interrupt. Stopping ..." << std::endl << std::endl;
    should_stop_.store(true);
}

/**
* @brief A thread worker dedicated at recovering public keys from
* transaction signatures
*/
class RecoveryWorker final : public silkworm::Worker {
  public:

    RecoveryWorker(uint32_t id, size_t data_size) : id_(id), data_size_{data_size} {};

    // Recovery package
    struct package {
        uint64_t block_num;
        ethash::hash256 hash;
        uint8_t recovery_id;
        uint8_t signature[64];
    };

    // Provides a container of packages to process
    void set_work(uint32_t batch_id, std::vector<package>& packages) {
        std::unique_lock l{xwork_};
        work_set_.swap(packages);
        batch_id_ = batch_id;
        Worker::kick();
    }

    uint32_t get_id() const { return id_; };
    uint32_t get_batch_id() const { return batch_id_; };
    std::string get_error(void) const { return last_error_; };

    // Pulls results from worker
    std::vector<std::pair<uint64_t, MDB_val>>& get_results(void) { return results_; };

    // Signal to connected handlers the task has completed
    boost::signals2::signal<void(uint32_t sender_id, uint32_t batch_id)> signal_completed;

   private:
     const uint32_t id_;                                    // Current worker identifier
     uint32_t batch_id_{0};                                 // Running batch identifier
     std::vector<package> work_set_{};                      // Work packages to process
     size_t data_size_;                                     // Size of the recovery data buffer
     uint8_t* data_{nullptr};                               // Pointer to data where rsults are stored
     std::vector<std::pair<uint64_t, MDB_val>> results_{};  // Results per block pointing to data area
     std::string last_error_{};                             // Description of last error occurrence

     // Basic work loop (overrides Worker::work())
     void work() final {

         // Try allocate enough memory to store
         // results output
         data_ = static_cast<uint8_t*>(std::calloc(1, data_size_));
         if (!data_) {
             throw std::runtime_error("Unable to allocate memory");
         }

         while (!should_stop()) {

             bool expected_kick_value{true};
             if (!kicked_.compare_exchange_strong(expected_kick_value, false, std::memory_order_relaxed)) {
                 std::unique_lock l(xwork_);
                 kicked_cv_.wait_for(l, std::chrono::seconds(1));
                 continue;
             }

             // Lock mutex so no other jobs may be set
             std::unique_lock l{xwork_};
             results_.clear();

             uint64_t block_num{work_set_.at(0).block_num};
             size_t block_result_offset{0};
             size_t block_result_length{0};

             // Loop
             for (auto const& package : work_set_) {
                 // On block switching store the results
                 if (block_num != package.block_num) {
                     MDB_val result{block_result_length, (void*)&data_[block_result_offset]};
                     results_.push_back({block_num, result});
                     block_result_offset += block_result_length;
                     block_result_length = 0;
                     block_num = package.block_num;
                     if (should_stop_) break;
                 }

                 std::optional<Bytes> recovered{
                     ecdsa::recover(full_view(package.hash.bytes), full_view(package.signature), package.recovery_id)};
                 if (recovered.has_value() && (int)recovered->at(0) == 4) {
                     auto keyHash{ethash::keccak256(recovered->data() + 1, recovered->length() - 1)};
                     std::memcpy(&data_[block_result_offset + block_result_length],
                                 &keyHash.bytes[sizeof(keyHash) - kAddressLength], kAddressLength);
                     block_result_length += kAddressLength;
                 } else {
                     last_error_ = "Public key recovery failed at block #" + std::to_string(package.block_num);
                     break;  // No need to process other txns
                 }
             }

             // Store results for last block
             if (block_result_length) {
                 MDB_val result{block_result_length, (void*)&data_[block_result_offset]};
                 results_.push_back({block_num, result});
             }

             // Raise finished event
             signal_completed(id_, batch_id_);
             work_set_.clear();  // Clear here. Next set_work will swap the cleaned container to master thread
             l.unlock();
         }

         std::free(data_);
    };
};

/**
* @brief An orchestrator of RecoveryWorkers
*/
class RecoveryFarm final
{
public:

  RecoveryFarm(uint32_t max_workers) : max_workers_{max_workers} {
      workers_.reserve(max_workers);
  };

  ~RecoveryFarm() = default;

  /**
  * @brief Recovers sender's public keys from transactions
  *
  * @param block_from : initial block to process transactions from
  * @param block_to : last block to process transactions
  * @param work_size : number of transactions to collect for every batch
  */
  int recover(lmdb::Transaction& db_transaction, uint64_t block_from, uint64_t block_to, size_t work_size) {



      work_set_.reserve(work_size);

  }

  /**
  * @brief Unwinds Sender's recovery stage
  */
  int unwind(lmdb::Transaction& db_transaction, uint64_t new_height) {

      int rc{0};
      SILKWORM_LOG(LogLevels::LogInfo) << "Unwinding Senders' table to height "  << new_height << std::endl;

      auto senders_table{db_transaction.open(db::table::kSenders, MDB_CREATE)};

      // Clear table if we unwind all
      if (new_height <= 1) {
          rc = senders_table->clear();
          try {
              lmdb::err_handler(senders_table->clear());
              db::stages::set_stage_progress(db_transaction, db::stages::kSendersKey, 0);
              SILKWORM_LOG(LogLevels::LogInfo) << "Done unwinding" << std::endl;
              rc = MDB_SUCCESS;
          } catch (const lmdb::exception& ec) {
              SILKWORM_LOG(LogLevels::LogError) << "Unexpected database error :  " << ec.what() << std::endl;
              rc = ec.err();
          }
          return rc;
      }

      Bytes key(40, '\0');
      boost::endian::store_big_u64(&key[0], new_height + 1);  // New stage height is last processed
      MDB_val mdb_key{db::to_mdb_val(key)}, mdb_data{};

      // Delete every record since initial key
      try {
          lmdb::err_handler(senders_table->seek(&mdb_key, &mdb_data));
          do {
              lmdb::err_handler(senders_table->del_current());
              lmdb::err_handler(senders_table->get_next(&mdb_key, &mdb_data));
          } while (true);
      } catch (const lmdb::exception& ec) {
          if (ec.err() != MDB_NOTFOUND) {
              SILKWORM_LOG(LogLevels::LogError) << "Unexpected database error :  " << ec.what() << std::endl;
              rc = ec.err();
          } else {
              rc = MDB_SUCCESS;
          }
      }

      // Update stage progress if everything ok
      if (!rc) {
          try {
              db::stages::set_stage_progress(db_transaction, db::stages::kSendersKey, new_height);
              SILKWORM_LOG(LogLevels::LogInfo) << "Done unwinding" << std::endl;
              rc = MDB_SUCCESS;
          } catch (const lmdb::exception& ec) {
              SILKWORM_LOG(LogLevels::LogError) << "Unexpected database error :  " << ec.what() << std::endl;
              rc = ec.err();
          }
      }

      return rc;
  }

protected:

    /**
    * @brief Gets executed by worker on its work completed
    */
    void worker_completed_handler(RecoveryWorker* sender, uint32_t completed_batch_num) {

        // Ensure threads flush in the right order to preserve key sorting
        // Threads waits for its ticket before flushing
        while (exp_completed_batch_num_.load(std::memory_order_relaxed) != completed_batch_num) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }

        // Store error condition if applicabile
        std::string recovery_error{ sender->get_error() };
        if (!recovery_error.empty()) {
            workers_thread_error_.store(true);
        }

        std::pair<uint32_t, uint32_t> batch_item{sender->get_id(), completed_batch_num};
        batches_completed.push(batch_item);
        exp_completed_batch_num_++;
        if (workers_in_flight_.load()) {
            workers_in_flight_--;
        }

    }

    std::vector<evmc::bytes32> load_canonical_headers(lmdb::Transaction& db_transaction, uint64_t block_from, uint64_t block_to) {

        const uint64_t count{block_to - block_from + 1};
        std::vector<evmc::bytes32> ret{};
        ret.reserve(count);

        SILKWORM_LOG(LogLevels::LogInfo) << "Loading canonical block headers [" << block_from << " ... " << block_to
                                         << "] ... " << std::endl;

        // Locate starting canonical block selected
        // and navigate headers
        auto headers_table{db_transaction.open(db::table::kBlockHeaders)};
        auto header_key{db::header_hash_key(block_from)};
        MDB_val mdb_key{db::to_mdb_val(header_key)}, mdb_data{};

        uint64_t expected_block_num{block_from};
        int rc{headers_table->seek_exact(&mdb_key, &mdb_data)};
        while (!rc) {
            if (mdb_key.mv_size == header_key.length() && mdb_data.mv_size) {
                ByteView data_view{static_cast<uint8_t*>(mdb_key.mv_data), mdb_key.mv_size};
                if (data_view[8] == 'n') {
                    auto reached_block_num{boost::endian::load_big_u64(&data_view[0])};
                    if (reached_block_num != expected_block_num) {

                    }
                }
            }
        }
    }

private:
  friend class RecoveryWorker;
  uint32_t max_workers_;                                    // Max number of workers/threads
  std::atomic_uint workers_in_flight_{0};                   // Counter of workers actually busy
  std::atomic_bool workers_error_{false};                   // Whether or not any worker is in error
  std::vector<std::unique_ptr<RecoveryWorker>> workers_{};  // Actual collection of recoverers
  std::vector<RecoveryWorker::package> work_set_{};         // Where to store work packages for RecoveryWorkers
  std::atomic_uint exp_completed_batch_num_{0};             // Batch identifier sent to RecoveryWorker thread
  std::queue<std::pair<uint32_t, uint32_t>>
      batches_completed{};  // Queue of batches completed waiting to be written on disk
};

void process_txs_for_signing(ChainConfig& config, uint64_t block_num, std::vector<silkworm::Transaction>& transactions,
                             std::vector<RecoveryWorker::package>& packages) {
    for (const auto& txn : transactions) {
        if (!silkworm::ecdsa::is_valid_signature(txn.r, txn.s, config.has_homestead(block_num))) {
            throw std::runtime_error("Got invalid signature in tx for block number " + std::to_string(block_num));
        }

        ecdsa::RecoveryId x{ecdsa::get_signature_recovery_id(txn.v)};

        Bytes rlp{};
        if (x.eip155_chain_id) {
            if (!config.has_spurious_dragon(block_num)) {
                throw std::runtime_error("EIP-155 signature in tx before Spurious Dragon for block number " +
                                         std::to_string(block_num));
            } else if (x.eip155_chain_id != config.chain_id) {
                throw std::runtime_error("Got invalid EIP-155 signature in tx for block number " +
                                         std::to_string(block_num) + " chain_id : expected " +
                                         std::to_string(config.chain_id) + " got " +
                                         intx::to_string(*x.eip155_chain_id));
            }
            rlp::encode(rlp, txn, true, {config.chain_id});
        } else {
            rlp::encode(rlp, txn, true, {});
        }

        auto hash{keccak256(rlp)};
        RecoveryWorker::package rp{block_num, hash, x.recovery_id};
        intx::be::unsafe::store(rp.signature, txn.r);
        intx::be::unsafe::store(rp.signature + 32, txn.s);
        packages.push_back(rp);
    }
}

bool start_workers(std::vector<std::unique_ptr<RecoveryWorker>>& workers) {
    for (const auto& worker : workers) {
        SILKWORM_LOG(LogLevels::LogInfo) << "Starting worker thread #" << worker->get_id() << std::endl;
        worker->start();
        // Wait for thread to init properly
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        if (worker->get_state() != Worker::WorkerState::kStarted) {
            return false;
        }
    }
    return true;
}

void stop_workers(std::vector<std::unique_ptr<RecoveryWorker>>& workers, bool wait) {
    for (const auto& worker : workers) {
        SILKWORM_LOG(LogLevels::LogInfo) << "Stopping worker thread #" << worker->get_id() << std::endl;
        worker->stop(wait);
    }
}

std::vector<evmc::bytes32> load_canonical_headers(lmdb::Transaction& txn, uint64_t from, uint64_t to) {
    uint64_t count{to - from + 1};
    SILKWORM_LOG(LogLevels::LogInfo) << "Loading canonical block headers [" << from << " ... " << to << "]"
                                     << std::endl;

    std::vector<evmc::bytes32> ret;
    ret.reserve(count);

    // Locate starting canonical block selected
    // and navigate headers
    auto headers_table{txn.open(db::table::kBlockHeaders)};
    auto header_key{db::header_hash_key(from)};
    MDB_val mdb_key{db::to_mdb_val(header_key)}, mdb_data{};

    uint32_t percent{0};
    uint32_t percent_step{5};  // 5% increment among batches
    size_t batch_size{count / (100 / percent_step)};

    int rc{headers_table->seek_exact(&mdb_key, &mdb_data)};
    while (!rc) {
        // Canonical header key is 9 bytes (8 blocknumber + 'n')
        if (mdb_key.mv_size == header_key.length() && mdb_data.mv_size) {
            ByteView v{static_cast<uint8_t*>(mdb_key.mv_data), mdb_key.mv_size};
            if (v[8] == 'n') {
                auto header_block{boost::endian::load_big_u64(&v[0])};
                if (header_block > to) {
                    rc = MDB_NOTFOUND;
                    break;
                } else {
                    ret.push_back(to_bytes32({static_cast<uint8_t*>(mdb_data.mv_data), mdb_data.mv_size}));
                    batch_size--;
                }
            }

            if (!batch_size) {
                batch_size = count / (100 / percent_step);
                percent += percent_step;
                SILKWORM_LOG(LogLevels::LogInfo)
                    << "... " << std::right << std::setw(3) << std::setfill(' ') << percent << " %" << std::endl;
                if (should_stop_) {
                    rc = MDB_NOTFOUND;
                    continue;
                }
            }
        }
        rc = (should_stop_ ? MDB_NOTFOUND : headers_table->get_next(&mdb_key, &mdb_data));
    }
    if (rc && rc != MDB_NOTFOUND) {
        lmdb::err_handler(rc);
    }

    if (should_stop_) {
        return {};
    }
    return ret;
}

// Writes batch results to db
size_t bufferize_results(std::queue<std::pair<uint32_t, uint32_t>>& batches, std::mutex& batches_mtx,
                         std::vector<std::unique_ptr<RecoveryWorker>>& workers,
                         std::vector<evmc::bytes32>::iterator& headers, etl::Collector& collector) {
    size_t ret{0};
    std::vector<std::pair<uint64_t, MDB_val>> results{};
    do {
        // Loop all completed batches until queue
        // empty. Other batches may complete while
        // writing of batch is in progress
        std::unique_lock l{batches_mtx};
        if (batches.empty()) {
            break;
        }

        // Pull result from proper worker
        auto& item{batches.front()};
        results.swap(workers.at(item.first)->get_results());
        batches.pop();
        l.unlock();

        // Bufferize results
        // Note ! Blocks arrive already sorted
        for (auto& [block_num, mdb_val] : results) {
            auto etl_key{db::block_key(block_num, headers->bytes)};
            Bytes etl_data(static_cast<unsigned char*>(mdb_val.mv_data), mdb_val.mv_size);
            etl::Entry etl_entry{etl_key, etl_data};
            collector.collect(etl_entry);
            headers++;
        }

        std::vector<std::pair<uint64_t, MDB_val>>().swap(results);

    } while (true);

    return ret;
}

// Unwinds Senders' table
void do_unwind(std::unique_ptr<lmdb::Transaction>& txn, uint64_t new_height) {
    SILKWORM_LOG(LogLevels::LogInfo) << "Unwinding Senders' table ... " << std::endl;
    auto senders{txn->open(db::table::kSenders, MDB_CREATE)};
    if (new_height <= 1) {
        lmdb::err_handler(senders->clear());
        return;
    }

    Bytes senders_key(40, '\0');
    boost::endian::store_big_u64(&senders_key[0], new_height);

    MDB_val key{}, data{};
    key.mv_data = (void*)&senders_key[0];
    key.mv_size = senders_key.length();
    int rc{senders->seek(&key, &data)};
    while (rc == MDB_SUCCESS) {
        lmdb::err_handler(senders->del_current());
        rc = senders->get_next(&key, &data);
    }
    if (rc != MDB_NOTFOUND) {
        lmdb::err_handler(rc);
    }
}

// Executes the recovery stage
int do_recover(app_options_t& options) {
    std::shared_ptr<lmdb::Environment> lmdb_env{nullptr};  // Main lmdb environment
    std::unique_ptr<lmdb::Transaction> lmdb_txn{nullptr};  // Main lmdb transaction
    ChainConfig config{kMainnetConfig};                    // Main net config flags
    std::vector<RecoveryWorker::package> work_set{};            // Where to store work packages for RecoveryWorkers
    work_set.reserve(options.batch_size);

    uint32_t next_batch_id{0};                   // Batch identifier sent to RecoveryWorker thread
    std::atomic<uint32_t> expected_batch_id{0};  // Holder of queue flushing order
    std::queue<std::pair<uint32_t, uint32_t>>
        batches_completed{};           // Queue of batches completed waiting to be written on disk
    std::mutex batches_completed_mtx;  // Guards the queue
    uint32_t next_worker_id{0};        // Used to serialize the dispatch of works to threads
    uint32_t max_workers{std::max(1u, std::thread::hardware_concurrency() - 1)};

    uint64_t total_transactions{0};  // Overall number of transactions processed

    // RecoveryWorker's signal handlers
    boost::function<void(uint32_t, uint32_t)> finishedHandler =
        [&expected_batch_id, &batches_completed, &batches_completed_mtx](
            uint32_t sender_id, uint32_t batch_id) {

            // Ensure threads flush in the right order to preserve key sorting
            // Threads waits for its ticket before flushing
            while (expected_batch_id.load(std::memory_order_relaxed) != batch_id) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }

            // Store error condition if applicabile
            if (error.err) {
                workers_thread_error_.store(true);
            }

            // Save my ids in the queue of results to
            // store in db
            std::unique_lock l{batches_completed_mtx};
            std::pair<uint32_t, uint32_t> item{sender_id, batch_id};
            batches_completed.push(item);

            // Ready to serve next thread
            expected_batch_id++;
            workers_in_flight--;
        };

    // Each RecoveryWorker will allocate enough
    // storage space to hold results for
    // a full batch. Worker object is not copyable
    // thus the need of a unique_ptr.
    std::vector<std::unique_ptr<RecoveryWorker>> RecoveryWorkers_{};
    for (uint32_t i = 0; i < RecoveryWorkers_.capacity(); i++) {
        RecoveryWorkers_.emplace_back(new RecoveryWorker(i, (options.batch_size * kAddressLength)));
        RecoveryWorkers_.back()->signal_completed.connect(boost::bind(finishedHandler, _1, _2, _3));
    }

    // Start RecoveryWorkers (here occurs allocation)
    if (!start_workers(RecoveryWorkers_)) {
        SILKWORM_LOG(LogLevels::LogCritical) << "Unable to start required RecoveryWorkers" << std::endl;
        stop_workers(RecoveryWorkers_, true);
        RecoveryWorkers_.clear();
        return -1;
    }

    // Initialize db_options
    lmdb::DatabaseConfig db_config{options.datadir};
    db_config.set_readonly(false);
    db_config.map_size = options.mapsize;

    // Compute etl temporary path
    fs::path db_path(options.datadir);
    fs::path etl_path(db_path.parent_path() / fs::path("etl-temp"));
    fs::create_directories(etl_path);
    etl::Collector collector(etl_path.string().c_str(), /* flush size */ 512 * kMebi);

    try {
        // Open db and start transaction
        lmdb_env = lmdb::get_env(db_config);
        lmdb_txn = lmdb_env->begin_rw_transaction();

        auto headers_stage_height{db::stages::get_stage_progress(*lmdb_txn, db::stages::kHeadersKey)};
        auto senders_stage_height{db::stages::get_stage_progress(*lmdb_txn, db::stages::kSendersKey)};

        SILKWORM_LOG(LogLevels::LogDebug) << "Headers Stage height " << senders_stage_height << std::endl;
        SILKWORM_LOG(LogLevels::LogDebug) << "Senders Stage height " << senders_stage_height << std::endl;

        // If requested replay then initial block can be anything in [1 ... stage_headers_height]
        // Otherwise is fixed to this stage height + 1
        if (options.replay) {
            options.block_from = std::min(options.block_from, (uint32_t)senders_stage_height + 1u);
        } else {
            options.block_from = (uint32_t)senders_stage_height + 1u;
        }
        options.block_to = std::max(options.block_from, options.block_to);
        if (options.block_to > headers_stage_height) {
            options.block_to = headers_stage_height;
        }

        if (options.block_from >= options.block_to) {
            throw std::logic_error("No headers to process");
        }

        // Do we have to unwind Sender's table ?
        if (options.block_from <= senders_stage_height) {
            uint64_t new_height{options.block_from <= 1 ? 0ull : uint64_t(options.block_from) - 1};
            do_unwind(lmdb_txn, new_height);
            SILKWORM_LOG(LogLevels::LogInfo) << "New stage height " << new_height << std::endl;
            db::stages::set_stage_progress(*lmdb_txn, db::stages::kSendersKey, new_height);
            lmdb::err_handler(lmdb_txn->commit());
            lmdb_txn = lmdb_env->begin_rw_transaction();
        }

        // Load all canonical headers
        auto canonical_headers{load_canonical_headers(*lmdb_txn, options.block_from, options.block_to)};
        if (!canonical_headers.size()) {
            // Nothing to process
            throw std::logic_error("No canonical headers collected.");
        }

        SILKWORM_LOG(LogLevels::LogInfo) << "Collected " << canonical_headers.size() << " canonical headers"
                                         << std::endl;

        {

            uint64_t block_num{0};                            // Block number being processed
            uint64_t expected_block_num{options.block_from};  // Expected block number in sequence
            auto header_read_it{canonical_headers.begin()};   // Iterator over canonical headers
            auto header_write_it{canonical_headers.begin()};  // Iterator over canonical headers


            SILKWORM_LOG(LogLevels::LogInfo) << "Scanning bodies ... " << std::endl;

            auto bodies_table{lmdb_txn->open(db::table::kBlockBodies)};
            auto transactions_table{lmdb_txn->open(db::table::kEthTx)};

            // Set to first block
            auto block_key{db::block_key(options.block_from, header_read_it->bytes)};
            MDB_val mdb_key{db::to_mdb_val(block_key)}, mdb_data{};
            int rc{bodies_table->seek_exact(&mdb_key, &mdb_data)};

            while (!rc) {
                auto key_view{db::from_mdb_val(mdb_key)};
                block_num = boost::endian::load_big_u64(key_view.data());

                if (block_num < expected_block_num) {
                    // The same block height has been recorded
                    // but is not canonical;
                    rc = should_stop_ ? MDB_NOTFOUND : bodies_table->get_next(&mdb_key, &mdb_data);
                    continue;
                } else if (block_num > expected_block_num) {
                    // We surpassed the expected block which means
                    // either the db misses a block or blocks are not persisted
                    // in sequence
                    throw std::runtime_error("Bad block body sequence. Expected " + std::to_string(expected_block_num) +
                                             " got " + std::to_string(block_num));
                }

                if (memcmp((void*)&key_view[8], (void*)header_read_it->bytes, 32) != 0) {
                    // We stumbled into a non canonical block (not matching header)
                    // move next and repeat
                    rc = should_stop_ ? MDB_NOTFOUND : bodies_table->get_next(&mdb_key, &mdb_data);
                    continue;
                }

                auto block_body{db::detail::decode_stored_block_body(db::from_mdb_val(mdb_data))};

                // We get here with a matching block number + header
                // Process it if not empty (ie 0 transactions and 0 ommers)
                if (block_body.txn_count) {

                    // Should we overflow the batch queue dispatch the work
                    // accumulated so far to the RecoveryWorker thread
                    if ((work_set.size() + block_body.txn_count) > options.batch_size) {

                        // If all workers busy no other option than to wait for
                        // at least one free slot
                        while (workers_in_flight == max_workers) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(500));
                        }

                        // Throw if any error from workers
                        if (workers_thread_error_) {
                            throw std::runtime_error("Error from worker thread");
                        }

                        // Write results to db (if any)
                        bufferize_results(batches_completed, batches_completed_mtx, RecoveryWorkers_, header_write_it,
                                          collector);

                        // Dispatch new task to worker
                        total_transactions += work_set.size();
                        RecoveryWorkers_.at(next_worker_id)->set_work(next_batch_id++, work_set);
                        workers_in_flight++;

                        SILKWORM_LOG(LogLevels::LogInfo)
                            << "Block " << std::right << std::setw(9) << std::setfill(' ') << block_num
                            << " Transactions " << std::right << std::setw(12) << std::setfill(' ')
                            << total_transactions << " Workers " << workers_in_flight << "/" << RecoveryWorkers_.capacity()
                            << std::endl;

                        if (++next_worker_id == RecoveryWorkers_.capacity()) {
                            next_worker_id = 0;
                        }
                    }

                    // Load transactions
                    std::vector<Transaction> transactions{
                        db::read_transactions(*transactions_table, block_body.base_txn_id, block_body.txn_count)};

                    // Enqueue Txs in current batch
                    process_txs_for_signing(config, block_num, transactions, work_set);
                }

                // After processing move to next block number and header
                if (++header_read_it == canonical_headers.end()) {
                    // We'd go beyond collected canonical headers
                    break;
                }

                expected_block_num++;
                rc = should_stop_ ? MDB_NOTFOUND : bodies_table->get_next(&mdb_key, &mdb_data);
            }
            if (rc && rc != MDB_NOTFOUND) {
                lmdb::err_handler(rc);
            }

            // Save last processed block
            options.block_to = block_num;

            // Should we have a partially filled work package deliver it now
            if (work_set.size() && !should_stop_) {
                // If all workers busy no other option than to wait for
                // at least one free slot
                while (workers_in_flight == RecoveryWorkers_.capacity()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                }

                // Throw if any error from workers
                if (workers_thread_error_) {
                    throw std::runtime_error("Error from worker thread");
                }

                // Write results to db (if any)
                bufferize_results(batches_completed, batches_completed_mtx, RecoveryWorkers_, header_write_it, collector);

                total_transactions += work_set.size();
                RecoveryWorkers_.at(next_worker_id)->set_work(next_batch_id, work_set);
                workers_in_flight++;

                SILKWORM_LOG(LogLevels::LogInfo)
                    << "Block " << std::right << std::setw(9) << std::setfill(' ') << block_num << " Transactions "
                    << std::right << std::setw(12) << std::setfill(' ') << total_transactions << " Workers "
                    << workers_in_flight << "/" << RecoveryWorkers_.capacity() << std::endl;
            }

            // Wait for all workers to complete and write their results
            while (workers_in_flight != 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            if (!should_stop_) {
                bufferize_results(batches_completed, batches_completed_mtx, RecoveryWorkers_, header_write_it, collector);
            }
        }

        SILKWORM_LOG(LogLevels::LogInfo) << "Bodies scan " << (should_stop_ ? "aborted! " : "completed!") << std::endl;

    } catch (lmdb::exception& ex) {
        // This handles specific lmdb errors
        SILKWORM_LOG(LogLevels::LogCritical) << "Unexpected error : " << ex.err() << " " << ex.what() << std::endl;
        main_thread_error_ = true;
    } catch (std::logic_error& ex) {
        SILKWORM_LOG(LogLevels::LogWarn) << ex.what() << std::endl;
        main_thread_error_ = true;
    } catch (std::runtime_error& ex) {
        // This handles runtime logic errors
        // eg. trying to open two rw txns
        SILKWORM_LOG(LogLevels::LogCritical) << "Unexpected error : " << ex.what() << std::endl;
        main_thread_error_ = true;
    }

    // Stop all RecoveryWorkers & free memory
    stop_workers(RecoveryWorkers_, true);

    // Should we commit ?
    if (!main_thread_error_ && !workers_thread_error_ && !options.rundry && !should_stop_) {
        SILKWORM_LOG(LogLevels::LogInfo) << "Loading data ..." << std::endl;
        try {
            // Load collected data into Senders' table
            auto senders_table{lmdb_txn->open(db::table::kSenders)};
            collector.load(senders_table.get(), nullptr, MDB_APPEND, /* log_every_percent = */ 10);
            db::stages::set_stage_progress(*lmdb_txn, db::stages::kSendersKey, options.block_to);
            lmdb::err_handler(lmdb_txn->commit());
            if ((db_config.flags & MDB_NOSYNC) == MDB_NOSYNC) {
                lmdb::err_handler(lmdb_env->sync());
            }
        } catch (const std::exception& ex) {
            SILKWORM_LOG(LogLevels::LogCritical) << " Unexpected error : " << ex.what() << std::endl;
            main_thread_error_ = true;
        }
    }

    lmdb_txn.reset();
    lmdb_env->close();
    lmdb_env.reset();

    SILKWORM_LOG(LogLevels::LogInfo) << "All done ! " << std::endl;
    return (main_thread_error_ ? -1 : 0);
}

// Prints out info of block's transactions with senders
int do_verify(app_options_t& options) {
    // Adjust params
    if (options.block_to == UINT32_MAX) options.block_to = options.block_from;

    try {
        // Open db and start transaction
        lmdb::DatabaseConfig db_config{options.datadir};
        db_config.map_size = options.mapsize;
        std::shared_ptr<lmdb::Environment> lmdb_env{lmdb::get_env(db_config)};
        std::unique_ptr<lmdb::Transaction> lmdb_txn{lmdb_env->begin_ro_transaction()};
        std::unique_ptr<lmdb::Table> lmdb_headers{lmdb_txn->open(db::table::kBlockHeaders)};
        std::unique_ptr<lmdb::Table> lmdb_bodies{lmdb_txn->open(db::table::kBlockBodies)};
        std::unique_ptr<lmdb::Table> lmdb_senders{lmdb_txn->open(db::table::kSenders)};

        // Verify requested block is not beyond what we already have in chaindb
        size_t count{0};
        lmdb::err_handler(lmdb_senders->get_rcount(&count));
        if (!count) throw std::logic_error("Senders table is empty. Is the sync completed ?");
        lmdb::err_handler(lmdb_bodies->get_rcount(&count));
        if (!count) throw std::logic_error("Block bodies table is empty. Is the sync completed ?");
        lmdb::err_handler(lmdb_headers->get_rcount(&count));
        if (!count) throw std::logic_error("Headers table is empty. Is the sync completed ?");

        MDB_val key, data;
        lmdb::err_handler(lmdb_senders->get_last(&key, &data));
        ByteView v{static_cast<uint8_t*>(key.mv_data), key.mv_size};
        uint64_t most_recent_sender{boost::endian::load_big_u64(&v[0])};
        if (options.block_from > most_recent_sender) {
            throw std::logic_error("Selected block beyond collected senders");
        }

        for (uint32_t block_num = options.block_from; block_num <= options.block_to; block_num++) {
            std::cout << "Reading block #" << block_num << std::endl;
            std::optional<BlockWithHash> bh{db::read_block(*lmdb_txn, block_num, /*read_senders=*/true)};
            if (!bh) {
                throw std::logic_error("Could not locate block #" + std::to_string(block_num));
            }

            if (!bh->block.transactions.size()) {
                std::cout << "Block has 0 transactions" << std::endl;
                continue;
            }

            std::cout << std::right << std::setw(4) << std::setfill(' ') << "Tx"
                      << " " << std::left << std::setw(66) << std::setfill(' ') << "Hash"
                      << " " << std::left << std::setw(42) << std::setfill(' ') << "From"
                      << " " << std::left << std::setw(42) << std::setfill(' ') << "To" << std::endl;
            std::cout << std::right << std::setw(4) << std::setfill('-') << ""
                      << " " << std::left << std::setw(66) << std::setfill('-') << ""
                      << " " << std::left << std::setw(42) << std::setfill('-') << ""
                      << " " << std::left << std::setw(42) << std::setfill('-') << "" << std::endl;

            for (size_t i = 0; i < bh->block.transactions.size(); i++) {
                Bytes rlp{};
                rlp::encode(rlp, bh->block.transactions.at(i), /*forsigning*/ false, {});
                ethash::hash256 hash{ethash::keccak256(rlp.data(), rlp.length())};
                ByteView bv{hash.bytes, 32};
                std::cout << std::right << std::setw(4) << std::setfill(' ') << i << " 0x" << to_hex(bv) << " 0x"
                          << to_hex(*(bh->block.transactions.at(i).from)) << " 0x"
                          << to_hex(*(bh->block.transactions.at(i).to)) << std::endl;
            }

            std::cout << std::endl;
        }

    } catch (const std::logic_error& ex) {
        std::cout << ex.what() << std::endl;
        return -1;
    } catch (const std::exception& ex) {
        std::cout << "Unexpected error : " << ex.what() << std::endl;
        return -1;
    }

    return 0;
}

int main(int argc, char* argv[]) {
    // Init command line parser
    CLI::App app("Senders recovery tool.");
    app_options_t options{};
    options.datadir = silkworm::db::default_path();  // Default chain data db path

    // Command line arguments
    app.add_option("--datadir", options.datadir, "Path to chain db", true)->check(CLI::ExistingDirectory);

    std::string mapSizeStr{"0"};
    app.add_option("--lmdb.mapSize", mapSizeStr, "Lmdb map size", true);
    app.add_option("--batch", options.batch_size, "Number of transactions to process per batch", true)
        ->check(CLI::Range((size_t)1'000, (size_t)10'000'000));
    app.add_option("--from", options.block_from, "Initial block number to process (inclusive)", true)
        ->check(CLI::Range(1u, UINT32_MAX));
    app.add_option("--to", options.block_to, "Final block number to process (inclusive)", true)
        ->check(CLI::Range(1u, UINT32_MAX));
    app.add_flag("--debug", options.debug, "May print some debug/trace info.");
    app.add_flag("--replay", options.replay, "Replay transactions.");
    app.add_flag("--dry", options.rundry, "Runs the full cycle but nothing is persisted");

    app.require_subcommand(1);  // One of the following subcommands is required
    auto& app_recover = *app.add_subcommand("recover", "Recovers senders' addresses");
    auto& app_verify = *app.add_subcommand("verify", "Verifies senders' addresses for given block");

    CLI11_PARSE(app, argc, argv);
    Logger::default_logger().set_local_timezone(true);  // for compatibility with TG logging
    if (options.debug) {
        Logger::default_logger().verbosity = LogLevels::LogTrace;
    }

    auto lmdb_mapSize{parse_size(mapSizeStr)};
    if (!lmdb_mapSize) {
        std::cerr << "Provided --lmdb.mapSize \"" << mapSizeStr << "\" is invalid" << std::endl;
        return -1;
    }
    if (*lmdb_mapSize) {
        // Adjust mapSize to a multiple of page_size
        size_t host_page_size{boost::interprocess::mapped_region::get_page_size()};
        options.mapsize = ((*lmdb_mapSize + host_page_size - 1) / host_page_size) * host_page_size;
    }
    if (!options.block_from) options.block_from = 1u;  // Block 0 (genesis) has no transactions

    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    // If database path is provided (and has passed CLI::ExistingDirectory validator
    // check whether it is empty
    fs::path db_path = fs::path(options.datadir);
    if (!fs::exists(db_path) || !fs::is_directory(db_path) || fs::is_empty(db_path)) {
        std::cerr << "Invalid or empty --datadir \"" << options.datadir << "\"" << std::endl
                  << "Try --help for help" << std::endl;
        return -1;
    } else {
        fs::path db_file = fs::path(db_path / fs::path("data.mdb"));
        if (!fs::exists(db_file) || !fs::file_size(db_file)) {
            std::cerr << "Invalid or empty data file \"" << db_file.string() << "\"" << std::endl
                      << "Try --help for help" << std::endl;
            return -1;
        }
    }

    // Enable debug logging if required
    if (options.debug) {
        Logger::default_logger().verbosity = LogLevels::LogTrace;
    }

    // Invoke proper action
    int rc{-1};
    if (app_recover) {
        rc = do_recover(options);
    } else if (app_verify) {
        rc = do_verify(options);
    } else {
        std::cerr << "No command specified" << std::endl;
    }

    return rc;
}
