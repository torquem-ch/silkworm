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

#include "silkworm_tg_api.h"

#include <cassert>

#include <gsl/gsl_util>

#include <silkworm/chain/config.hpp>
#include <silkworm/common/log.hpp>
#include <silkworm/common/magic_enum.hpp>
#include <silkworm/db/access_layer.hpp>
#include <silkworm/db/buffer.hpp>
#include <silkworm/execution/execution.hpp>

SILKWORM_EXPORT SilkwormStatusData silkworm_execute_blocks(MDB_txn* mdb_txn, uint64_t chain_id, uint64_t start_block,
                                                           uint64_t max_block, uint64_t batch_size,
                                                           bool write_receipts) SILKWORM_NOEXCEPT {
    assert(mdb_txn);

    SilkwormStatusData status_data = {
        .code = SilkwormStatusCode::kSilkwormSuccess, .last_executed_block = 0, .lmdb_error_code = 0};

    using namespace silkworm;

    const ChainConfig* config{lookup_chain_config(chain_id)};
    if (!config) {
        SILKWORM_LOG(LogLevel::Error) << "Unsupported chain ID " << chain_id << std::endl;
        status_data.code = SilkwormStatusCode::kSilkwormUnknownChainId;
        return status_data;
    }

    uint64_t block_num{start_block};

    try {
        lmdb::Transaction txn{/*parent=*/nullptr, mdb_txn, /*flags=*/0};
        auto cleanup{gsl::finally([&txn] { *txn.handle() = nullptr; })};  // avoid aborting mdb_txn

        db::Buffer buffer{&txn};
        AnalysisCache analysis_cache;
        ExecutionStatePool state_pool;

        for (; block_num <= max_block; ++block_num) {
            std::optional<BlockWithHash> bh{db::read_block(txn, block_num, /*read_senders=*/true)};
            if (!bh) {
                status_data.code = SilkwormStatusCode::kSilkwormBlockNotFound;
                return status_data;
            }

            auto [receipts, err]{execute_block(bh->block, buffer, *config, &analysis_cache, &state_pool)};
            if (err != ValidationResult::kOk) {
                SILKWORM_LOG(LogLevel::Error)
                    << "Validation error " << static_cast<int>(err) << " at block " << block_num << std::endl;
                status_data.code = SilkwormStatusCode::kSilkwormInvalidBlock;
                return status_data;
            }

            if (write_receipts) {
                buffer.insert_receipts(block_num, receipts);
            }

            status_data.last_executed_block = block_num;

            if (block_num % 1000 == 0) {
                SILKWORM_LOG(LogLevel::Info) << "Blocks <= " << block_num << " executed" << std::endl;
            }

            if (buffer.current_batch_size() >= batch_size) {
                buffer.write_to_db();
                status_data.code = SilkwormStatusCode::kSilkwormSuccess;
                return status_data;
            }
        };

        buffer.write_to_db();
        return status_data;
    } catch (const lmdb::exception& e) {
        status_data.lmdb_error_code = e.err();
        SILKWORM_LOG(LogLevel::Error) << "LMDB error " << e.what() << std::endl;
        status_data.code = SilkwormStatusCode::kSilkwormLmdbError;
        return status_data;
    } catch (const db::MissingSenders&) {
        SILKWORM_LOG(LogLevel::Error) << "Missing or incorrect senders at block " << block_num << std::endl;
        status_data.code = SilkwormStatusCode::kSilkwormMissingSenders;
        return status_data;
    } catch (const rlp::DecodingError& ex) {
        SILKWORM_LOG(LogLevel::Error) << ex.what() << " at block " << block_num << std::endl;
        status_data.code = SilkwormStatusCode::kSilkwormDecodingError;
        return status_data;
    } catch (...) {
        SILKWORM_LOG(LogLevel::Error) << "Unkown error at block " << block_num << std::endl;
        status_data.code = SilkwormStatusCode::kSilkwormUnknownError;
        return status_data;
    }
}
