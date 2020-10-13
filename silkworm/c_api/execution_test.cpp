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

#include "execution.h"

#include <catch2/catch.hpp>
#include <ethash/keccak.hpp>
#include <silkworm/chain/config.hpp>
#include <silkworm/common/temp_dir.hpp>
#include <silkworm/db/access_layer.hpp>
#include <silkworm/db/chaindb.hpp>
#include <silkworm/db/tables.hpp>
#include <silkworm/execution/address.hpp>
#include <silkworm/execution/protocol_param.hpp>
#include <silkworm/rlp/encode.hpp>
#include <silkworm/types/account.hpp>
#include <silkworm/types/block.hpp>

TEST_CASE("Execution API") {
    using namespace silkworm;

    TemporaryDirectory tmp_dir{};
    lmdb::options db_opts{};
    db_opts.map_size = 32 << 20;  //  32MiB
    db_opts.read_only = false;
    std::shared_ptr<lmdb::Environment> db_env{lmdb::get_env(tmp_dir.path(), db_opts)};
    std::unique_ptr<lmdb::Transaction> txn{db_env->begin_rw_transaction()};

    uint64_t block_number{1};

    uint64_t chain_id{404};
    CHECK(silkworm_execute_block(*txn->handle(), chain_id, block_number, nullptr) == kSilkwormUnknownChainId);

    chain_id = kMainnetConfig.chain_id;
    int lmdb_error_code{MDB_SUCCESS};
    CHECK(silkworm_execute_block(*txn->handle(), chain_id, block_number, &lmdb_error_code) == kSilkwormLmdbError);
    CHECK(lmdb_error_code == MDB_NOTFOUND);

    db::table::create_all(*txn);
    CHECK(silkworm_execute_block(*txn->handle(), chain_id, block_number, &lmdb_error_code) == kSilkwormBlockNotFound);

    // ---------------------------------------
    // First block
    // ---------------------------------------

    auto miner{0x5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c_address};

    Block block{};
    block.header.number = block_number;
    block.header.beneficiary = miner;
    block.header.gas_limit = 100'000;

    // This contract initially sets its 0th storage to 0x2a
    // and its 1st storage to 0x01c9.
    // When called, it updates its 0th storage to the input provided.
    Bytes contract_code{from_hex("600035600055")};
    Bytes deployment_code{from_hex("602a6000556101c960015560068060166000396000f3") + contract_code};

    block.transactions.resize(1);
    block.transactions[0].data = deployment_code;
    block.transactions[0].gas_limit = block.header.gas_limit;
    block.transactions[0].gas_price = 20 * kGiga;

    Bytes header_rlp{};
    rlp::encode(header_rlp, block.header);
    ethash::hash256 block_hash{keccak256(header_rlp)};

    auto header_table{txn->open(db::table::kBlockHeaders)};
    header_table->put(db::header_hash_key(block_number), full_view(block_hash.bytes));
    Bytes block_key{db::block_key(block_number, block_hash.bytes)};
    header_table->put(block_key, header_rlp);

    Bytes rubbish{from_hex("86438ddee4412f")};
    auto body_table{txn->open(db::table::kBlockBodies)};
    body_table->put(block_key, rubbish);

    CHECK(silkworm_execute_block(*txn->handle(), chain_id, block_number, &lmdb_error_code) == kSilkwormDecodingError);

    const BlockBody& block_body{block};
    Bytes body_rlp{};
    rlp::encode(body_rlp, block_body);
    body_table->put(block_key, body_rlp);

    CHECK(silkworm_execute_block(*txn->handle(), chain_id, block_number, &lmdb_error_code) == kSilkwormMissingSenders);

    auto sender{0xb685342b8c54347aad148e1f22eff3eb3eb29391_address};
    auto sender_table{txn->open(db::table::kSenders)};
    sender_table->put(block_key, full_view(sender));

    CHECK(silkworm_execute_block(*txn->handle(), chain_id, block_number, &lmdb_error_code) == kSilkwormInvalidBlock);

    Account sender_account{};
    sender_account.balance = kEther;
    auto state_table{txn->open(db::table::kPlainState)};
    state_table->put(full_view(sender), sender_account.encode_for_storage(/*omit_code_hash=*/false));

    REQUIRE(silkworm_execute_block(*txn->handle(), chain_id, block_number, &lmdb_error_code) == kSilkwormSuccess);

    auto contract_address{create_address(sender, /*nonce=*/0)};
    std::optional<Account> contract_account{db::read_account(*txn, contract_address, block_number)};
    REQUIRE(contract_account);

    ethash::hash256 code_hash{keccak256(contract_code)};
    CHECK(to_hex(contract_account->code_hash) == to_hex(full_view(code_hash.bytes)));

    uint64_t incarnation{1};

    evmc::bytes32 storage_key0{};
    evmc::bytes32 storage0{db::read_storage(*txn, contract_address, incarnation, storage_key0, block_number)};
    CHECK(to_hex(storage0) == "000000000000000000000000000000000000000000000000000000000000002a");

    evmc::bytes32 storage_key1{to_bytes32(from_hex("01"))};
    evmc::bytes32 storage1{db::read_storage(*txn, contract_address, incarnation, storage_key1, block_number)};
    CHECK(to_hex(storage1) == "00000000000000000000000000000000000000000000000000000000000001c9");

    std::optional<Account> miner_account{db::read_account(*txn, miner)};
    REQUIRE(miner_account);
    CHECK(miner_account->balance > 1 * param::kFrontierBlockReward);
    CHECK(miner_account->balance < 2 * param::kFrontierBlockReward);

    // ---------------------------------------
    // Second block
    // ---------------------------------------

    std::string new_val{"000000000000000000000000000000000000000000000000000000000000003e"};

    block_number = 2;
    block.header.number = block_number;
    block.transactions[0].nonce = 1;
    block.transactions[0].to = contract_address;
    block.transactions[0].data = from_hex(new_val);

    header_rlp.clear();
    rlp::encode(header_rlp, block.header);
    block_hash = keccak256(header_rlp);
    header_table->put(db::header_hash_key(block_number), full_view(block_hash.bytes));

    block_key = db::block_key(block_number, block_hash.bytes);
    header_table->put(block_key, header_rlp);

    body_rlp.clear();
    const BlockBody& block_body2{block};
    rlp::encode(body_rlp, block_body2);
    body_table->put(block_key, body_rlp);
    sender_table->put(block_key, full_view(sender));

    REQUIRE(silkworm_execute_block(*txn->handle(), chain_id, block_number, &lmdb_error_code) == kSilkwormSuccess);

    storage0 = db::read_storage(*txn, contract_address, incarnation, storage_key0, block_number);
    CHECK(to_hex(storage0) == new_val);

    miner_account = db::read_account(*txn, miner);
    REQUIRE(miner_account);
    CHECK(miner_account->balance > 2 * param::kFrontierBlockReward);
    CHECK(miner_account->balance < 3 * param::kFrontierBlockReward);

    // ---------------------------------------
    // Check change sets
    // ---------------------------------------

    std::optional<db::AccountChanges> account_changes{db::read_account_changes(*txn, 1)};
    REQUIRE(account_changes);
    CHECK(account_changes->size() == 3);

    // sender existed at genesis
    CHECK(!account_changes->at(sender).empty());

    // miner & contract were created in block 1
    CHECK(account_changes->at(miner).empty());
    CHECK(account_changes->at(contract_address).empty());

    Bytes storage_changes_encoded{db::read_storage_changes(*txn, 1)};
    db::StorageChanges storage_changes_expected{};
    storage_changes_expected[db::storage_key(contract_address, incarnation, storage_key0)] = {};
    storage_changes_expected[db::storage_key(contract_address, incarnation, storage_key1)] = {};
    CHECK(storage_changes_encoded == storage_changes_expected.encode());

    storage_changes_encoded = db::read_storage_changes(*txn, 2);
    storage_changes_expected.clear();
    storage_changes_expected[db::storage_key(contract_address, incarnation, storage_key0)] = from_hex("2a");
    CHECK(storage_changes_encoded == storage_changes_expected.encode());
}