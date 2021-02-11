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

#ifndef SILKWORM_DB_ACCESS_LAYER_H_
#define SILKWORM_DB_ACCESS_LAYER_H_

// Database Access Layer
// See TG core/rawdb/accessors_chain.go

#include <optional>
#include <silkworm/db/chaindb.hpp>
#include <silkworm/db/util.hpp>
#include <silkworm/types/account.hpp>
#include <silkworm/types/block.hpp>
#include <silkworm/types/receipt.hpp>
#include <vector>

namespace silkworm::db {

class MissingSenders : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// See TG StorageModeReceipts
constexpr const char* kStorageModeReceipts{"smReceipts"};

// See TG GetStorageModeFromDB
bool read_storage_mode_receipts(lmdb::Transaction& txn);

std::optional<BlockHeader> read_header(lmdb::Transaction& txn, uint64_t block_number,
                                       const uint8_t (&hash)[kHashLength]);

std::optional<BlockBody> read_body(lmdb::Transaction& txn, uint64_t block_number, const uint8_t (&hash)[kHashLength],
                                   bool read_senders);

// See TG ReadTd
std::optional<intx::uint256> read_total_difficulty(lmdb::Transaction& txn, uint64_t block_number,
                                                   const uint8_t (&hash)[kHashLength]);

// See TG ReadBlockByNumber
// might throw MissingSenders
std::optional<BlockWithHash> read_block(lmdb::Transaction& txn, uint64_t block_number, bool read_senders);

// See TG ReadSenders
std::vector<evmc::address> read_senders(lmdb::Transaction& txn, int64_t block_number,
                                        const uint8_t (&hash)[kHashLength]);

// Overload
std::vector<Transaction> read_transactions(lmdb::Table& txn_table, uint64_t base_id, uint64_t count);

std::optional<Bytes> read_code(lmdb::Transaction& txn, const evmc::bytes32& code_hash);

// Reads current or historical (if block_number is specified) account.
std::optional<Account> read_account(lmdb::Transaction& txn, const evmc::address& address,
                                    std::optional<uint64_t> block_number = std::nullopt);

// Reads current or historical (if block_number is specified) storage.
evmc::bytes32 read_storage(lmdb::Transaction& txn, const evmc::address& address, uint64_t incarnation,
                           const evmc::bytes32& location, std::optional<uint64_t> block_number = std::nullopt);

// Reads current or historical (if block_number is specified) previous incarnation.
std::optional<uint64_t> read_previous_incarnation(lmdb::Transaction& txn, const evmc::address& address,
                                                  std::optional<uint64_t> block_number = std::nullopt);

AccountChanges read_account_changes(lmdb::Transaction& txn, uint64_t block_number);

StorageChanges read_storage_changes(lmdb::Transaction& txn, uint64_t block_number);

bool migration_happened(lmdb::Transaction& txn, const char* name);

// See TG AppendReceipts in core/rawdb/accessors_chain.go
void append_receipts(lmdb::Transaction& txn, uint64_t block_number, const std::vector<Receipt>& receipts);

}  // namespace silkworm::db

#endif  // SILKWORM_DB_ACCESS_LAYER_H_
