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

#include "buffer.hpp"

#include <absl/container/btree_set.h>

#include <algorithm>
#include <boost/endian/conversion.hpp>
#include <silkworm/common/util.hpp>

#include "access_layer.hpp"
#include "tables.hpp"

namespace silkworm::db {

void Buffer::bump_batch_size(size_t key_len, size_t value_len) {
    // Approximately matches TG batch size logic in (m *mutation) Put
    static constexpr size_t kEntryOverhead{8};
    batch_size_ += kEntryOverhead + key_len + value_len;
}

void Buffer::begin_block(uint64_t block_number) {
    block_number_ = block_number;
    changed_storage_.clear();
}

void Buffer::update_account(const evmc::address& address, std::optional<Account> initial,
                            std::optional<Account> current) {
    bool equal{current == initial};
    bool account_deleted{!current};

    if (equal && !account_deleted && !changed_storage_.contains(address)) {
        // Follows the Turbo-Geth logic when to populate account changes.
        // See (ChangeSetWriter)UpdateAccountData & DeleteAccount.
        return;
    }

    Bytes encoded_initial{};
    if (initial) {
        bool omit_code_hash{!account_deleted};
        encoded_initial = initial->encode_for_storage(omit_code_hash);
    }
    if (account_changes_[block_number_].insert_or_assign(address, encoded_initial).second) {
        bump_batch_size(8, kAddressLength + encoded_initial.length());
    }

    if (equal) {
        return;
    }

    if (accounts_.insert_or_assign(address, current).second) {
        bump_batch_size(kAddressLength, current ? current->encoding_length_for_storage() : 0);
    };

    if (account_deleted && initial->incarnation) {
        if (incarnations_.insert_or_assign(address, initial->incarnation).second) {
            bump_batch_size(kAddressLength, kIncarnationLength);
        }
    }
}

void Buffer::update_account_code(const evmc::address& address, uint64_t incarnation, const evmc::bytes32& code_hash,
                                 ByteView code) {
    if (hash_to_code_.insert_or_assign(code_hash, code).second) {
        bump_batch_size(kHashLength, code.length());
    }
    if (storage_prefix_to_code_hash_.insert_or_assign(storage_prefix(address, incarnation), code_hash).second) {
        bump_batch_size(kStoragePrefixLength, kHashLength);
    }
}

void Buffer::update_storage(const evmc::address& address, uint64_t incarnation, const evmc::bytes32& location,
                            const evmc::bytes32& initial, const evmc::bytes32& current) {
    if (current == initial) {
        return;
    }
    changed_storage_.insert(address);
    ByteView change_val{zeroless_view(initial)};
    if (storage_changes_[block_number_][address][incarnation].insert_or_assign(location, change_val).second) {
        bump_batch_size(8 + kStoragePrefixLength, kHashLength + change_val.size());
    }

    if (storage_[address][incarnation].insert_or_assign(location, current).second) {
        bump_batch_size(kStoragePrefixLength, kHashLength + zeroless_view(current).size());
    }
}

static void upsert_storage_value(lmdb::Table& state_table, ByteView storage_prefix, const evmc::bytes32& location,
                                 const evmc::bytes32& value) {
    state_table.del(storage_prefix, full_view(location));
    if (!is_zero(value)) {
        Bytes data{full_view(location)};
        data.append(zeroless_view(value));
        state_table.put(storage_prefix, data);
    }
}

void Buffer::write_to_state_table() {
    auto state_table{txn_->open(table::kPlainState)};

    // sort before inserting into the DB
    absl::btree_set<evmc::address> addresses;
    for (auto& x : accounts_) {
        addresses.insert(x.first);
    }
    for (auto& x : storage_) {
        addresses.insert(x.first);
    }

    std::vector<evmc::bytes32> storage_keys;

    for (const auto& address : addresses) {
        if (auto it{accounts_.find(address)}; it != accounts_.end()) {
            state_table->del(full_view(address));
            if (it->second.has_value()) {
                bool omit_code_hash{false};
                Bytes encoded{it->second->encode_for_storage(omit_code_hash)};
                state_table->put(full_view(address), encoded);
            }
        }

        if (auto it{storage_.find(address)}; it != storage_.end()) {
            for (const auto& contract : it->second) {
                uint64_t incarnation{contract.first};
                Bytes prefix{storage_prefix(address, incarnation)};

                const auto& contract_storage{contract.second};

                // sort before inserting into the DB
                storage_keys.clear();
                for (const auto& x : contract_storage) {
                    storage_keys.push_back(x.first);
                }
                std::sort(storage_keys.begin(), storage_keys.end());

                for (const auto& k : storage_keys) {
                    upsert_storage_value(*state_table, prefix, k, contract_storage.at(k));
                }
            }
        }
    }
}

void Buffer::write_to_db() {
    if (!txn_) {
        return;
    }

    write_to_state_table();

    auto incarnation_table{txn_->open(table::kIncarnationMap)};
    Bytes data(kIncarnationLength, '\0');
    for (const auto& entry : incarnations_) {
        boost::endian::store_big_u64(&data[0], entry.second);
        incarnation_table->put(full_view(entry.first), data);
    }

    auto code_table{txn_->open(table::kCode)};
    for (const auto& entry : hash_to_code_) {
        code_table->put(full_view(entry.first), entry.second);
    }

    auto code_hash_table{txn_->open(table::kPlainContractCode)};
    for (const auto& entry : storage_prefix_to_code_hash_) {
        code_hash_table->put(entry.first, full_view(entry.second));
    }

    auto account_change_table{txn_->open(table::kPlainAccountChangeSet)};
    Bytes change_key;
    for (const auto& block_entry : account_changes_) {
        uint64_t block_num{block_entry.first};
        change_key = block_key(block_num);
        for (const auto& account_entry : block_entry.second) {
            data = full_view(account_entry.first);
            data.append(account_entry.second);
            account_change_table->put(change_key, data);
        }
    }

    auto storage_change_table{txn_->open(table::kPlainStorageChangeSet)};
    for (const auto& block_entry : storage_changes_) {
        uint64_t block_num{block_entry.first};
        for (const auto& address_entry : block_entry.second) {
            const evmc::address& address{address_entry.first};
            for (const auto& incarnation_entry : address_entry.second) {
                uint64_t incarnation{incarnation_entry.first};
                change_key = storage_change_key(block_num, address, incarnation);
                for (const auto& storage_entry : incarnation_entry.second) {
                    data = full_view(storage_entry.first);
                    data.append(storage_entry.second);
                    storage_change_table->put(change_key, data);
                }
            }
        }
    }
}

evmc::bytes32 Buffer::state_root_hash() const { throw std::runtime_error("not yet implemented"); }

uint64_t Buffer::current_canonical_block() const { throw std::runtime_error("not yet implemented"); }

std::optional<evmc::bytes32> Buffer::canonical_hash(uint64_t) const { throw std::runtime_error("not yet implemented"); }

void Buffer::canonize_block(uint64_t, const evmc::bytes32&) { throw std::runtime_error("not yet implemented"); }

void Buffer::decanonize_block(uint64_t) { throw std::runtime_error("not yet implemented"); }

void Buffer::insert_block(const Block& block, const evmc::bytes32& hash) {
    uint64_t block_number{block.header.number};
    Bytes key{block_key(block_number, hash.bytes)};
    headers_[key] = block.header;
    bodies_[key] = block;

    if (block_number == 0) {
        difficulty_[key] = 0;
    } else {
        std::optional<intx::uint256> parent_difficulty{total_difficulty(block_number - 1, block.header.parent_hash)};
        difficulty_[key] = parent_difficulty.value_or(0);
    }
    difficulty_[key] += block.header.difficulty;
}

std::optional<intx::uint256> Buffer::total_difficulty(uint64_t block_number,
                                                      const evmc::bytes32& block_hash) const noexcept {
    Bytes key{block_key(block_number, block_hash.bytes)};
    if (auto it{difficulty_.find(key)}; it != difficulty_.end()) {
        return it->second;
    }
    if (!txn_) {
        return std::nullopt;
    }
    return db::read_total_difficulty(*txn_, block_number, block_hash.bytes);
}

std::optional<BlockHeader> Buffer::read_header(uint64_t block_number, const evmc::bytes32& block_hash) const noexcept {
    Bytes key{block_key(block_number, block_hash.bytes)};
    if (auto it{headers_.find(key)}; it != headers_.end()) {
        return it->second;
    }
    if (!txn_) {
        return std::nullopt;
    }
    return db::read_header(*txn_, block_number, block_hash.bytes);
}

std::optional<BlockBody> Buffer::read_body(uint64_t block_number, const evmc::bytes32& block_hash) const noexcept {
    Bytes key{block_key(block_number, block_hash.bytes)};
    if (auto it{bodies_.find(key)}; it != bodies_.end()) {
        return it->second;
    }
    if (!txn_) {
        return std::nullopt;
    }
    return db::read_body(*txn_, block_number, block_hash.bytes, /*read_senders=*/false);
}

std::optional<Account> Buffer::read_account(const evmc::address& address) const noexcept {
    if (auto it{accounts_.find(address)}; it != accounts_.end()) {
        return it->second;
    }
    if (!txn_) {
        return std::nullopt;
    }
    return db::read_account(*txn_, address, historical_block_);
}

Bytes Buffer::read_code(const evmc::bytes32& code_hash) const noexcept {
    if (auto it{hash_to_code_.find(code_hash)}; it != hash_to_code_.end()) {
        return it->second;
    }
    if (!txn_) {
        return {};
    }
    std::optional<Bytes> code{db::read_code(*txn_, code_hash)};
    if (code) {
        return *code;
    } else {
        return {};
    }
}

evmc::bytes32 Buffer::read_storage(const evmc::address& address, uint64_t incarnation,
                                   const evmc::bytes32& location) const noexcept {
    if (auto it1{storage_.find(address)}; it1 != storage_.end()) {
        if (auto it2{it1->second.find(incarnation)}; it2 != it1->second.end()) {
            if (auto it3{it2->second.find(location)}; it3 != it2->second.end()) {
                return it3->second;
            }
        }
    }

    if (!txn_) {
        return {};
    }
    return db::read_storage(*txn_, address, incarnation, location, historical_block_);
}

uint64_t Buffer::previous_incarnation(const evmc::address& address) const noexcept {
    if (auto it{incarnations_.find(address)}; it != incarnations_.end()) {
        return it->second;
    }
    if (!txn_) {
        return 0;
    }
    std::optional<uint64_t> incarnation{db::read_previous_incarnation(*txn_, address, historical_block_)};
    return incarnation ? *incarnation : 0;
}

void Buffer::unwind_state_changes(uint64_t) { throw std::runtime_error("not yet implemented"); }

}  // namespace silkworm::db
