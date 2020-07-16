/*
   Copyright 2020 The Silkworm Authors

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

#ifndef SILKWORM_DB_DATABASE_H_
#define SILKWORM_DB_DATABASE_H_

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "change.hpp"
#include "types/account.hpp"
#include "types/block.hpp"

namespace silkworm::db {

struct Entry {
  std::string_view key;
  std::string_view value;
};

class Cursor {
 public:
  Cursor(const Cursor&) = delete;
  Cursor& operator=(const Cursor&) = delete;

  virtual ~Cursor() = default;

  virtual std::optional<Entry> seek(std::string_view prefix) = 0;

 protected:
  Cursor() = default;
};

class Bucket {
 public:
  Bucket(const Bucket&) = delete;
  Bucket& operator=(const Bucket&) = delete;

  virtual ~Bucket() = default;

  virtual void put(std::string_view key, std::string_view value) = 0;

  virtual std::optional<std::string_view> get(std::string_view key) const = 0;

  virtual std::unique_ptr<Cursor> cursor() = 0;

 protected:
  Bucket() = default;
};

class Transaction {
 public:
  Transaction(const Transaction&) = delete;
  Transaction& operator=(const Transaction&) = delete;

  virtual ~Transaction() = default;

  virtual std::unique_ptr<Bucket> create_bucket(const char* name) = 0;
  virtual std::unique_ptr<Bucket> get_bucket(const char* name) = 0;

  virtual void commit() = 0;
  virtual void rollback() = 0;

 protected:
  Transaction() = default;
};

class Database {
 public:
  Database(const Database&) = delete;
  Database& operator=(const Database&) = delete;

  virtual ~Database() = default;

  virtual std::unique_ptr<Transaction> begin_transaction(bool read_only) = 0;

  std::unique_ptr<Transaction> begin_ro_transaction() { return begin_transaction(true); }
  std::unique_ptr<Transaction> begin_rw_transaction() { return begin_transaction(false); }

  std::optional<BlockWithHash> get_block(uint64_t block_number);
  std::vector<evmc::address> get_senders(uint64_t block_number, const evmc::bytes32& block_hash);
  std::string get_code(const evmc::bytes32& code_hash);
  std::optional<Account> get_account(const evmc::address& address, uint64_t block_number);
  std::optional<AccountChanges> get_account_changes(uint64_t block_number);
  std::string get_storage_changes(uint64_t block_number);
  evmc::bytes32 get_storage(const evmc::address& address, uint64_t incarnation,
                            const evmc::bytes32& key, uint64_t block_number);

 protected:
  Database() = default;

 private:
  // Turbo-Geth FindByHistory
  std::optional<std::string_view> find_in_history(Transaction& txn, bool storage,
                                                  std::string_view key, uint64_t block_number);
};
}  // namespace silkworm::db

#endif  // SILKWORM_DB_DATABASE_H_
