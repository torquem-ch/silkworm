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

#ifndef SILKWORM_TG_API_H_
#define SILKWORM_TG_API_H_

// C API exported by Silkworm to be used in Erigon.

#include <lmdb.h>
#include <stdbool.h>
#include <stdint.h>

#if defined _MSC_VER
#define SILKWORM_EXPORT __declspec(dllexport)
#else
#define SILKWORM_EXPORT __attribute__((visibility("default")))
#endif

#if __cplusplus
#define SILKWORM_NOEXCEPT noexcept
#else
#define SILKWORM_NOEXCEPT
#endif

#if __cplusplus
extern "C" {
#endif

enum SilkwormStatusCode {
    kSilkwormSuccess = 0,
    kSilkwormBlockNotFound = 1,
    kSilkwormUnknownChainId = 2,
    kSilkwormLmdbError = 3,
    kSilkwormMissingSenders = 4,
    kSilkwormInvalidBlock = 5,
    kSilkwormDecodingError = 6,
    kSilkwormIncompatibleDbFormat = 7,
    kSilkwormUnknownError = -1
};

typedef enum SilkwormStatusCode SilkwormStatusCode_t;

struct SilkwormStatusData {
    SilkwormStatusCode_t code;
    uint64_t last_executed_block;
    int lmdb_error_code;
};

/** @brief Executes a batch of Ethereum blocks and writes resulting changes into the database.
 *
 * @param[in] txn Valid read-write LMDB transaction. Must not be NULL.
 * This function does not commit nor abort the transaction.
 * @param[in] chain_id EIP-155 chain ID. kSilkwormUnknownChainId is returned in case of an unknown or unsupported chain.
 * @param[in] start_block The block height to start the execution from.
 * @param[in] max_block Do not execute after this block.
 * max_block may be executed, or the execution may stop earlier if the batch is full.
 * @param[in] batch_size The size of DB changes to accumulate before returning from this method.
 * Pass 0 if you want to execute just 1 block.
 * @param[in] write_receipts Whether to write CBOR-encoded receipts into the DB.
 *
 * @param[out] last_executed_block The height of the last successfully executed block.
 * Not written to if no blocks were executed, otherwise *last_executed_block ≤ max_block.
 * @param[out] lmdb_error_code If an LMDB error occurs (this function returns kSilkwormLmdbError)
 * and lmdb_error_code isn't NULL, it's populated with the relevant LMDB error code.
 *
 * @return A non-zero error value on failure and kSilkwormSuccess(=0) on success.
 * kSilkwormBlockNotFound is probably OK: it simply means that the execution reached the end of the chain
 * (blocks up to and incl. last_executed_block were still executed).
 */
SILKWORM_EXPORT struct SilkwormStatusData silkworm_execute_blocks(MDB_txn* txn, uint64_t chain_id, uint64_t start_block,
                                                                  uint64_t max_block, uint64_t batch_size,
                                                                  bool write_receipts) SILKWORM_NOEXCEPT;

#if __cplusplus
}
#endif

#endif  // SILKWORM_TG_API_H_
