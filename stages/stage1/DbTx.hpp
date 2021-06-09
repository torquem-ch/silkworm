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
#ifndef SILKWORM_DBTX_HPP
#define SILKWORM_DBTX_HPP

#include <silkworm/db/access_layer.hpp>
#include <silkworm/db/stages.hpp>
#include <silkworm/db/util.hpp>
#include <silkworm/db/tables.hpp>
#include "Types.hpp"

using namespace silkworm;

class DbTx {
    std::shared_ptr<lmdb::Environment> env;
    std::unique_ptr<lmdb::Transaction> txn;

  public:
    explicit DbTx(std::string db_path) {
        lmdb::DatabaseConfig db_config{db_path};
        //db_config.set_readonly(false);
        env = lmdb::get_env(db_config);
        txn = env->begin_ro_transaction();
    }

    std::optional<Hash> read_canonical_hash(BlockNum b) {  // throws db exceptions // todo: add to db::access_layer.hpp?
        auto hashes_table = txn->open(db::table::kCanonicalHashes);
        // accessing this table with only b we will get the hash of the canonical block at height b
        std::optional<ByteView> hash = hashes_table->get(db::block_key(b));
        if (!hash) return std::nullopt; // not found
        assert(hash->size() == kHashLength);
        return hash.value(); // copy
    }

    static Bytes head_header_key() { // todo: add to db::util.h?
        std::string table_name = db::table::kHeadHeader.name;
        Bytes key{table_name.begin(), table_name.end()};
        return key;
    }

    std::optional<Hash> read_head_header_hash() { // todo: add to db::access_layer.hpp?
        auto head_header_table = txn->open(db::table::kHeadHeader);
        std::optional<ByteView> hash = head_header_table->get(head_header_key());
        if (!hash) return std::nullopt; // not found
        assert(hash->size() == kHashLength);
        return hash.value(); // copy
    }

    std::optional<BlockHeader> read_header(BlockNum b, Hash h)  {
        return db::read_header(*txn, b, h.bytes);
    }

    std::optional<BlockHeader> read_canonical_header(BlockNum b)  { // also known as read-header-by-number
        std::optional<Hash> h = read_canonical_hash(b);
        if (!h) return std::nullopt; // not found
        return read_header(b, *h);
    }

    std::optional<ByteView> read_rlp_encoded_header(BlockNum b, Hash h)  {
        auto header_table = txn->open(db::table::kHeaders);
        std::optional<ByteView> rlp = header_table->get(db::block_key(b, h.bytes));
        return rlp;
    }

    static Bytes header_numbers_key(Hash h) { // todo: add to db::util.h?
        return {h.bytes, 32};
    }

    std::optional<BlockNum> read_block_num(Hash h) { // todo: add to db::access_layer.hpp?
        auto blockhashes_table = txn->open(db::table::kHeaderNumbers);
        auto encoded_block_num = blockhashes_table->get(header_numbers_key(h));
        if (!encoded_block_num) return {};
        BlockNum block_num = boost::endian::load_big_u64(encoded_block_num->data());
        return block_num;
    }

    std::optional<BlockHeader> read_header(Hash h) { // todo: add to db::access_layer.hpp?
        auto block_num = read_block_num(h);
        if (!block_num) return {};
        return read_header(*block_num, h);
    }

    std::optional<BlockBody> read_body(Hash h) { // todo: add to db::access_layer.hpp?
        auto block_num = read_block_num(h);
        if (!block_num) return {};
        bool read_senders = false;
        return db::read_body(*txn, *block_num, h.bytes, read_senders);
    }

    std::optional<intx::uint256> read_total_difficulty(BlockNum b, Hash h) {
        return db::read_total_difficulty(*txn, b, h.bytes);
    }

    BlockNum stage_progress(const char* stage_name) {
        return db::stages::get_stage_progress(*txn, stage_name);
    }
};

#endif  // SILKWORM_DBTX_HPP
