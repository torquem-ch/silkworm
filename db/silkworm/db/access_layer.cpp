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

#include "access_layer.hpp"

#include <cassert>

#include <boost/endian/conversion.hpp>
#include <nlohmann/json.hpp>

#include "bitmap.hpp"
#include "tables.hpp"

namespace silkworm::db {

std::optional<BlockHeader> read_header(mdbx::txn& txn, uint64_t block_number, const uint8_t (&hash)[kHashLength]) {
    auto src{db::open_cursor(txn, table::kHeaders)};
    auto key{block_key(block_number, hash)};
    auto data{src.find(to_slice(key), false)};
    if (!data) {
        return std::nullopt;
    }

    BlockHeader header;
    ByteView data_view{from_iovec(data.value)};
    rlp::err_handler(rlp::decode(data_view, header));
    return header;
}

std::optional<intx::uint256> read_total_difficulty(mdbx::txn& txn, uint64_t block_number,
                                                   const uint8_t (&hash)[kHashLength]) {
    auto src{db::open_cursor(txn, table::kDifficulty)};
    auto key{block_key(block_number, hash)};
    auto data{src.find(to_slice(key), false)};
    if (!data) {
        return std::nullopt;
    }
    intx::uint256 td{0};
    ByteView data_view{from_iovec(data.value)};
    rlp::err_handler(rlp::decode(data_view, td));
    return td;
}

// Erigon ReadTransactions
static std::vector<Transaction> read_transactions(mdbx::txn& txn, uint64_t base_id, uint64_t count) {
    if (!count) {
        return {};
    }
    auto src{db::open_cursor(txn, table::kEthTx)};
    return read_transactions(src, base_id, count);
}

std::vector<Transaction> read_transactions(mdbx::cursor& txn_table, uint64_t base_id, uint64_t count) {
    std::vector<Transaction> v;
    if (count == 0) {
        return v;
    }
    v.reserve(count);

    Bytes key(8, '\0');
    boost::endian::store_big_u64(key.data(), base_id);

    uint64_t i{0};
    for (auto data{txn_table.find(to_slice(key), false)}; data.done && i < count;
         data = txn_table.to_next(/*throw_notfound = */ false), ++i) {
        ByteView data_view{from_iovec(data.value)};
        Transaction eth_txn;
        rlp::err_handler(rlp::decode(data_view, eth_txn));
        v.push_back(eth_txn);
    }

    return v;
}

std::optional<BlockWithHash> read_block(mdbx::txn& txn, uint64_t block_number, bool read_senders) {
    // Locate canonical hash
    auto src{db::open_cursor(txn, table::kCanonicalHashes)};
    auto key{block_key(block_number)};
    auto data{src.find(to_slice(key), false)};
    if (!data) {
        return std::nullopt;
    }

    BlockWithHash bh{};
    assert(data.value.length() == kHashLength);
    std::memcpy(bh.hash.bytes, data.value.iov_base, kHashLength);

    // Locate header
    src = db::open_cursor(txn, table::kHeaders);
    key = block_key(block_number, bh.hash.bytes);
    data = src.find(to_slice(key), false);
    if (!data) {
        return std::nullopt;
    }

    ByteView data_view(from_iovec(data.value));
    rlp::err_handler(rlp::decode(data_view, bh.block.header));

    // Read body
    std::optional<BlockBody> body{read_body(txn, block_number, bh.hash.bytes, read_senders)};
    if (!body) {
        return std::nullopt;
    }

    std::swap(bh.block.ommers, body->ommers);
    std::swap(bh.block.transactions, body->transactions);

    return bh;
}

std::optional<BlockBody> read_body(mdbx::txn& txn, uint64_t block_number, const uint8_t (&hash)[kHashLength],
                                   bool read_senders) {
    auto src{db::open_cursor(txn, table::kBlockBodies)};
    auto key{block_key(block_number, hash)};
    auto data{src.find(to_slice(key), false)};
    if (!data) {
        return std::nullopt;
    }
    ByteView data_view{from_iovec(data.value)};
    auto body{detail::decode_stored_block_body(data_view)};

    BlockBody out;
    std::swap(out.ommers, body.ommers);
    out.transactions = read_transactions(txn, body.base_txn_id, body.txn_count);

    if (read_senders) {
        std::vector<evmc::address> senders{db::read_senders(txn, block_number, hash)};
        if (senders.size() != out.transactions.size()) {
            throw MissingSenders("senders count does not match transactions count");
        }
        for (size_t i{0}; i < senders.size(); ++i) {
            out.transactions[i].from = senders[i];
        }
    }

    return out;
}

std::vector<evmc::address> read_senders(mdbx::txn& txn, int64_t block_number, const uint8_t (&hash)[kHashLength]) {
    // TODO (Andrea) - Isn't this deprecated ?
    std::vector<evmc::address> senders{};

    auto src{db::open_cursor(txn, table::kBlockBodies)};
    auto key{block_key(block_number, hash)};
    auto data{src.find(to_slice(key), /*throw_notfound = */ false)};
    if (data) {
        assert(data.value.length() % kAddressLength == 0);
        senders.resize(data.value.length() / kAddressLength);
        std::memcpy(senders.data(), data.value.iov_base, data.value.length());
    }
    return senders;
}

std::optional<Bytes> read_code(mdbx::txn& txn, const evmc::bytes32& code_hash) {
    auto src{db::open_cursor(txn, table::kCode)};
    auto key{to_slice(full_view(code_hash))};
    auto data{src.find(key, /*throw_notfound=*/false)};
    if (!data) {
        return std::nullopt;
    }
    return Bytes{from_iovec(data.value)};
}

// Erigon FindByHistory for account
static std::optional<ByteView> historical_account(mdbx::txn& txn, const evmc::address& address, uint64_t block_number) {
    auto src{db::open_cursor(txn, table::kCode)};
    auto key{account_history_key(address, block_number)};
    auto data{src.find(to_slice(key), /*throw_notfound=*/false)};
    if (!data) {
        return std::nullopt;
    }
    db::Entry entry{from_iovec(data.key), from_iovec(data.value)};
    auto address_view{full_view(address)};
    if (!has_prefix(entry.key, address_view)) {
        return std::nullopt;
    }

    auto bitmap{bitmap::read(entry.value)};
    auto change_block{bitmap::seek(bitmap, block_number)};
    if (!change_block) {
        return std::nullopt;
    }

    src = db::open_cursor(txn, table::kPlainAccountChangeSet);
    key = block_key(*change_block);
    data = src.find_multivalue(to_slice(key), to_slice(address_view), /*throw_notfound=*/false);
    if (!data) {
        return std::nullopt;
    }
    return {from_iovec(data.value)};
}

// Erigon FindByHistory for storage
static std::optional<ByteView> historical_storage(mdbx::txn& txn, const evmc::address& address, uint64_t incarnation,
                                                  const evmc::bytes32& location, uint64_t block_number) {
    auto src{db::open_cursor(txn, table::kStorageHistory)};
    auto key{storage_history_key(address, location, block_number)};
    if (!src.seek(to_slice(key))) {
        return std::nullopt;
    }

    auto data{src.current(false)};
    auto k{from_iovec(data.key)};
    if (k.substr(0, kAddressLength) != full_view(address) ||
        k.substr(kAddressLength, kHashLength) != full_view(location)) {
        return std::nullopt;
    }

    auto bitmap{bitmap::read(from_iovec(data.value))};
    auto change_block{bitmap::seek(bitmap, block_number)};
    if (!change_block) {
        return std::nullopt;
    }

    src = db::open_cursor(txn, table::kPlainStorageChangeSet);
    key = storage_change_key(*change_block, address, incarnation);
    data = src.find_multivalue(to_slice(key), mdbx::slice(location.bytes, 32), /*throw_notfound*/ false);
    if (!data) {
        return std::nullopt;
    }
    return {from_iovec(data.value)};
}

std::optional<Account> read_account(mdbx::txn& txn, const evmc::address& address, std::optional<uint64_t> block_num) {
    std::optional<ByteView> encoded{block_num.has_value() ? historical_account(txn, address, block_num.value())
                                                          : std::nullopt};

    if (!encoded.has_value()) {
        auto src{db::open_cursor(txn, table::kPlainState)};
        if (auto data{src.find({address.bytes, sizeof(evmc::address)}, false)}; data.done) {
            encoded.emplace(from_iovec(data.value));
        }
    }
    if (!encoded.has_value() || encoded->empty()) {
        return std::nullopt;
    }

    auto [acc, err]{decode_account_from_storage(encoded.value())};
    rlp::err_handler(err);

    if (acc.incarnation > 0 && acc.code_hash == kEmptyHash) {
        // restore code hash
        auto src{db::open_cursor(txn, table::kPlainContractCode)};
        auto key{storage_prefix(full_view(address), acc.incarnation)};
        if (auto data{src.find(to_slice(key), /*throw_notfound*/ false)};
            data.done && data.value.length() == kHashLength) {
            std::memcpy(acc.code_hash.bytes, data.value.iov_base, kHashLength);
        }
    }

    return acc;
}

evmc::bytes32 read_storage(mdbx::txn& txn, const evmc::address& address, uint64_t incarnation,
                           const evmc::bytes32& location, std::optional<uint64_t> block_num) {
    std::optional<ByteView> val{block_num.has_value()
                                    ? historical_storage(txn, address, incarnation, location, block_num.value())
                                    : std::nullopt};
    if (!val.has_value()) {
        auto src{db::open_cursor(txn, table::kPlainState)};
        auto key{storage_prefix(full_view(address), incarnation)};
        if (auto data{src.find_multivalue(to_slice(key), mdbx::slice{location.bytes, sizeof(location)}, false)};
            data.done) {
            val.emplace(from_iovec(data.value));
        } else {
            return {};
        }
    }

    evmc::bytes32 res{};
    std::memcpy(res.bytes + kHashLength - val->length(), val->data(), val->length());
    return res;
}

static std::optional<uint64_t> historical_previous_incarnation() {
    // TODO(Andrew): implement properly
    return std::nullopt;
}

std::optional<uint64_t> read_previous_incarnation(mdbx::txn& txn, const evmc::address& address,
                                                  std::optional<uint64_t> block_num) {
    if (block_num.has_value()) {
        return historical_previous_incarnation();
    }

    auto src{db::open_cursor(txn, table::kIncarnationMap)};
    if (auto data{src.find(mdbx::slice{address.bytes, sizeof(evmc::address)}, /*throw_notfound*/ false)}; data.done) {
        assert(data.value.length() == 8);
        return boost::endian::load_big_u64(data.value.byte_ptr());
    }
    return std::nullopt;
}

AccountChanges read_account_changes(mdbx::txn& txn, uint64_t block_num) {
    AccountChanges changes;

    auto src{db::open_cursor(txn, table::kPlainAccountChangeSet)};
    auto key{block_key(block_num)};

    auto data{src.find(to_slice(key), /*throw_notfound*/ false)};
    while (data) {
        assert(data.value.length() >= kAddressLength);
        evmc::address address;
        std::memcpy(address.bytes, data.value.iov_base, kAddressLength);
        data.value.remove_prefix(kAddressLength);
        changes[address] = Bytes{data.value.byte_ptr(), data.value.length()};
        data = src.to_current_next_multi(/*throw_not_found*/ false);
    }

    return changes;
}

StorageChanges read_storage_changes(mdbx::txn& txn, uint64_t block_num) {
    StorageChanges changes;

    const Bytes block_prefix{block_key(block_num)};

    auto src{db::open_cursor(txn, table::kPlainStorageChangeSet)};

    auto key_prefix{to_slice(block_prefix)};
    auto data{src.lower_bound(key_prefix, false)};
    while (data) {

        if (!data.key.starts_with(key_prefix)) {
            break;
        }

        data.key.remove_prefix(key_prefix.length());
        assert(data.key.length() == kStoragePrefixLength);

        evmc::address address;
        std::memcpy(address.bytes, data.key.iov_base, kAddressLength);
        data.key.remove_prefix(kAddressLength);
        uint64_t incarnation{boost::endian::load_big_u64(data.key.byte_ptr())};

        assert(data.value.length() >= kHashLength);
        evmc::bytes32 location;
        std::memcpy(location.bytes, data.value.iov_base, kHashLength);
        data.value.remove_prefix(kHashLength);

        changes[address][incarnation][location] = Bytes{data.value.byte_ptr(), data.value.length()};
        data = src.to_next(/*throw_notfound=*/false);
    }

    return changes;
}

bool read_storage_mode_receipts(mdbx::txn& txn) {
    auto src{db::open_cursor(txn, table::kDatabaseInfo)};
    auto key{to_slice(byte_view_of_c_str(kStorageModeReceipts))};
    auto data{src.find(key, /*throw_notfound=*/false)};
    return (data.done && data.value.length() == 1 && static_cast<uint8_t>(data.value.at(0)) == 1);
}

bool migration_happened(mdbx::txn& txn, const char* name) {
    auto src{db::open_cursor(txn, table::kMigrations)};
    auto key{to_slice(byte_view_of_c_str(name))};
    auto data{src.find(key, /*throw_notfound=*/false)};
    return (data.done == true);
}

std::optional<ChainConfig> read_chain_config(mdbx::txn& txn) {
    auto src{db::open_cursor(txn, table::kCanonicalHashes)};
    auto key{to_slice(block_key(0))};
    auto data{src.find(key, /*throw_notfound=*/false)};
    if (!data) {
        return std::nullopt;
    }

    src = db::open_cursor(txn, table::kConfig);
    key = data.value;
    data = src.find(key, /*throw_notfound=*/false);
    if (!data) {
        return std::nullopt;
    }

    // https://github.com/nlohmann/json/issues/2204
    const auto json = nlohmann::json::parse(data.value.string(), nullptr, false);
    return ChainConfig::from_json(json);
}

}  // namespace silkworm::db
