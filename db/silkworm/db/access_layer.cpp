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

#include <boost/endian/conversion.hpp>
#include <cassert>
#include <silkworm/types/log_cbor.hpp>
#include <silkworm/types/receipt_cbor.hpp>

#include "history_index.hpp"
#include "tables.hpp"

namespace silkworm::db {

static void check_rlp_err(rlp::DecodingResult err) {
    if (err != rlp::DecodingResult::kOk) {
        throw err;
    }
}

std::optional<BlockHeader> read_header(lmdb::Transaction& txn, uint64_t block_number,
                                       const uint8_t (&hash)[kHashLength]) {
    auto table{txn.open(table::kBlockHeaders)};
    std::optional<ByteView> rlp{table->get(block_key(block_number, hash))};
    if (!rlp) {
        return std::nullopt;
    }

    BlockHeader header;
    check_rlp_err(rlp::decode(*rlp, header));
    return header;
}

std::optional<intx::uint256> read_total_difficulty(lmdb::Transaction& txn, uint64_t block_number,
                                                   const uint8_t (&hash)[kHashLength]) {
    auto table{txn.open(table::kBlockHeaders)};
    std::optional<ByteView> rlp{table->get(total_difficulty_key(block_number, hash))};
    if (!rlp) {
        return std::nullopt;
    }

    intx::uint256 td{0};
    check_rlp_err(rlp::decode(*rlp, td));
    return td;
}

// TG ReadTransactions
static std::vector<Transaction> read_transactions(lmdb::Transaction& txn, uint64_t base_id, uint64_t count) {
    if (!count) {
        return {};
    }

    auto table{txn.open(table::kEthTx)};
    return read_transactions(*table, base_id, count);
}

std::vector<Transaction> read_transactions(lmdb::Table& txn_table, uint64_t base_id, uint64_t count) {
    std::vector<Transaction> v;
    if (count == 0) {
        return v;
    }
    v.reserve(count);

    Bytes txn_key(8, '\0');
    boost::endian::store_big_u64(txn_key.data(), base_id);
    MDB_val key_mdb{to_mdb_val(txn_key)};
    MDB_val data_mdb{};

    uint64_t i{0};
    for (int rc{txn_table.seek_exact(&key_mdb, &data_mdb)}; rc != MDB_NOTFOUND && i < count;
         rc = txn_table.get_next(&key_mdb, &data_mdb), ++i) {
        lmdb::err_handler(rc);
        ByteView data{from_mdb_val(data_mdb)};

        Transaction eth_txn;
        check_rlp_err(rlp::decode(data, eth_txn));
        v.push_back(eth_txn);
    }

    return v;
}

std::optional<BlockWithHash> read_block(lmdb::Transaction& txn, uint64_t block_number, bool read_senders) {
    auto header_table{txn.open(table::kBlockHeaders)};
    std::optional<ByteView> hash{header_table->get(header_hash_key(block_number))};
    if (!hash) {
        return std::nullopt;
    }

    BlockWithHash bh{};
    assert(hash->size() == kHashLength);
    std::memcpy(bh.hash.bytes, hash->data(), kHashLength);

    Bytes key{block_key(block_number, bh.hash.bytes)};
    std::optional<ByteView> header_rlp{header_table->get(key)};
    if (!header_rlp) {
        return std::nullopt;
    }

    check_rlp_err(rlp::decode(*header_rlp, bh.block.header));

    std::optional<BlockBody> body{read_body(txn, block_number, bh.hash.bytes, read_senders)};
    if (!body) {
        return std::nullopt;
    }

    bh.block.ommers = body->ommers;
    bh.block.transactions = body->transactions;

    return bh;
}

std::optional<BlockBody> read_body(lmdb::Transaction& txn, uint64_t block_number, const uint8_t (&hash)[kHashLength],
                                   bool read_senders) {
    Bytes key{block_key(block_number, hash)};

    auto body_table{txn.open(table::kBlockBodies)};
    std::optional<ByteView> body_rlp{body_table->get(key)};
    if (!body_rlp) {
        return std::nullopt;
    }

    auto body{detail::decode_stored_block_body(*body_rlp)};

    BlockBody out;
    out.ommers = body.ommers;
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

std::vector<evmc::address> read_senders(lmdb::Transaction& txn, int64_t block_number,
                                        const uint8_t (&hash)[kHashLength]) {
    std::vector<evmc::address> senders{};
    auto table{txn.open(table::kSenders)};
    std::optional<ByteView> data{table->get(block_key(block_number, hash))};
    if (!data) {
        return senders;
    }

    assert(data->length() % kAddressLength == 0);
    senders.resize(data->length() / kAddressLength);
    std::memcpy(senders.data(), data->data(), data->size());
    return senders;
}

std::optional<Bytes> read_code(lmdb::Transaction& txn, const evmc::bytes32& code_hash) {
    auto table{txn.open(table::kCode)};
    std::optional<ByteView> val{table->get(full_view(code_hash))};
    if (!val) {
        return {};
    }
    return Bytes{*val};
}

// TG FindByHistory for account
static std::optional<ByteView> find_account_in_history(lmdb::Transaction& txn, const evmc::address& address,
                                                       uint64_t block_number) {
    auto history_table{txn.open(table::kAccountHistory)};
    std::optional<Entry> entry{history_table->seek(account_history_key(address, block_number))};
    if (!entry) {
        return std::nullopt;
    }

    ByteView k{entry->key};
    if (!has_prefix(k, full_view(address))) {
        return std::nullopt;
    }

    std::optional<history_index::SearchResult> res{history_index::find(entry->value, block_number)};
    if (!res) {
        return std::nullopt;
    }

    if (res->new_record) {
        return ByteView{};
    }

    auto change_table{txn.open(table::kPlainAccountChangeSet)};
    uint64_t change_block{res->change_block};
    return change_table->get(block_key(change_block), full_view(address));
}

// TG FindByHistory for storage
static std::optional<ByteView> find_storage_in_history(lmdb::Transaction& txn, const evmc::address& address,
                                                       uint64_t incarnation, const evmc::bytes32& location,
                                                       uint64_t block_number) {
    auto history_table{txn.open(table::kStorageHistory)};
    std::optional<Entry> entry{history_table->seek(storage_history_key(address, location, block_number))};
    if (!entry) {
        return std::nullopt;
    }

    ByteView k{entry->key};
    if (k.substr(0, kAddressLength) != full_view(address) ||
        k.substr(kAddressLength, kHashLength) != full_view(location)) {
        return std::nullopt;
    }

    std::optional<history_index::SearchResult> res{history_index::find(entry->value, block_number)};
    if (!res) {
        return std::nullopt;
    }

    auto change_table{txn.open(table::kPlainStorageChangeSet)};
    uint64_t change_block{res->change_block};
    return change_table->get(storage_change_key(change_block, address, incarnation), full_view(location));
}

std::optional<Account> read_account(lmdb::Transaction& txn, const evmc::address& address,
                                    std::optional<uint64_t> block_num) {
    std::optional<ByteView> encoded{};
    if (block_num) {
        encoded = find_account_in_history(txn, address, *block_num);
    }
    if (!encoded) {
        auto state_table{txn.open(table::kPlainState)};
        encoded = state_table->get(full_view(address));
    }
    if (!encoded || encoded->empty()) {
        return {};
    }

    auto [acc, err]{decode_account_from_storage(*encoded)};
    check_rlp_err(err);

    if (acc.incarnation > 0 && acc.code_hash == kEmptyHash) {
        // restore code hash
        auto code_hash_table{txn.open(table::kPlainContractCode)};
        std::optional<ByteView> hash{code_hash_table->get(storage_prefix(address, acc.incarnation))};
        if (hash && hash->length() == kHashLength) {
            std::memcpy(acc.code_hash.bytes, hash->data(), kHashLength);
        }
    }

    return acc;
}

evmc::bytes32 read_storage(lmdb::Transaction& txn, const evmc::address& address, uint64_t incarnation,
                           const evmc::bytes32& location, std::optional<uint64_t> block_num) {
    std::optional<ByteView> val{};
    if (block_num) {
        val = find_storage_in_history(txn, address, incarnation, location, *block_num);
    }
    if (!val) {
        auto table{txn.open(table::kPlainState)};
        val = table->get(storage_prefix(address, incarnation), full_view(location));
    }
    if (!val) {
        return {};
    }

    evmc::bytes32 res{};
    std::memcpy(res.bytes + kHashLength - val->length(), val->data(), val->length());
    return res;
}

std::optional<uint64_t> read_previous_incarnation(lmdb::Transaction& txn, const evmc::address& address,
                                                  std::optional<uint64_t> block_num) {
    if (!block_num) {
        // Current incarnation
        auto incarnation_table{txn.open(table::kIncarnationMap)};
        std::optional<ByteView> val{incarnation_table->get(full_view(address))};
        if (!val) {
            return {};
        }
        assert(val->length() == 8);
        return boost::endian::load_big_u64(val->data());
    }

    auto history_table{txn.open(table::kAccountHistory)};
    auto change_table{txn.open(table::kPlainAccountChangeSet)};

    // Search through history and find the latest non-zero incarnation of the account,
    // disregarding future changes (happening after the block_number).
    uint64_t block_number{*block_num};
    while (true) {
        std::optional<Entry> entry{history_table->seek(account_history_key(address, block_number))};
        if (!entry || !has_prefix(entry->key, full_view(address))) {
            return {};
        }

        std::optional<history_index::SearchResult> changed_at{history_index::find(entry->value, block_number)};
        if (!changed_at) {
            return {};
        }

        uint64_t change_block{changed_at->change_block};

        std::optional<ByteView> encoded{change_table->get(block_key(change_block), full_view(address))};
        if (encoded && !encoded->empty()) {
            auto [acc, err]{decode_account_from_storage(*encoded)};
            check_rlp_err(err);
            if (acc.incarnation > 0) {
                return acc.incarnation;
            }
        }

        // The account was deleted or had zero incarnation,
        // so go further back in time.
        changed_at = history_index::find_previous(entry->value, block_number);
        if (!changed_at) {
            return {};
        }
        block_number = changed_at->change_block;
    }
}

AccountChanges read_account_changes(lmdb::Transaction& txn, uint64_t block_num) {
    AccountChanges changes;
    auto table{txn.open(table::kPlainAccountChangeSet)};
    Bytes blck_key{block_key(block_num)};
    MDB_val key_mdb{to_mdb_val(blck_key)};
    MDB_val data_mdb;
    for (int rc{table->seek_exact(&key_mdb, &data_mdb)}; rc != MDB_NOTFOUND;
         rc = table->get_next_dup(&key_mdb, &data_mdb)) {
        lmdb::err_handler(rc);
        ByteView data{from_mdb_val(data_mdb)};
        assert(data.length() >= kAddressLength);
        evmc::address address;
        std::memcpy(address.bytes, data.data(), kAddressLength);
        data.remove_prefix(kAddressLength);
        changes[address] = data;
    }
    return changes;
}

StorageChanges read_storage_changes(lmdb::Transaction& txn, uint64_t block_num) {
    StorageChanges changes;
    auto table{txn.open(table::kPlainStorageChangeSet)};
    Bytes prefix{block_key(block_num)};
    MDB_val key_mdb{to_mdb_val(prefix)};
    MDB_val data_mdb;
    for (int rc{table->seek(&key_mdb, &data_mdb)}; rc != MDB_NOTFOUND; rc = table->get_next(&key_mdb, &data_mdb)) {
        lmdb::err_handler(rc);

        ByteView key{from_mdb_val(key_mdb)};
        if (!has_prefix(key, prefix)) {
            break;
        }
        key.remove_prefix(prefix.length());
        assert(key.length() == kStoragePrefixLength);
        evmc::address address;
        std::memcpy(address.bytes, key.data(), kAddressLength);
        key.remove_prefix(kAddressLength);
        uint64_t incarnation{boost::endian::load_big_u64(key.data())};

        ByteView data{from_mdb_val(data_mdb)};
        assert(data.length() >= kHashLength);
        evmc::bytes32 location;
        std::memcpy(location.bytes, data.data(), kHashLength);
        data.remove_prefix(kHashLength);

        changes[address][incarnation][location] = data;
    }
    return changes;
}

bool read_storage_mode_receipts(lmdb::Transaction& txn) {
    auto table{txn.open(table::kDatabaseInfo)};
    std::optional<ByteView> val{table->get(byte_view_of_c_str(kStorageModeReceipts))};
    return val && val->length() == 1 && (*val)[0] == 1;
}

bool migration_happened(lmdb::Transaction& txn, const char* name) {
    auto tbl{txn.open(table::kMigrations)};
    return tbl->get(byte_view_of_c_str(name)).has_value();
}

void append_receipts(lmdb::Transaction& txn, uint64_t block_number, const std::vector<Receipt>& receipts) {
    auto log_table{txn.open(table::kLogs)};
    for (uint32_t i{0}; i < receipts.size(); ++i) {
        if (!receipts[i].logs.empty()) {
            log_table->put(log_key(block_number, i), cbor_encode(receipts[i].logs), MDB_APPEND);
        }
    }

    auto receipt_table{txn.open(table::kBlockReceipts)};
    receipt_table->put(block_key(block_number), cbor_encode(receipts), MDB_APPEND);
}

}  // namespace silkworm::db
