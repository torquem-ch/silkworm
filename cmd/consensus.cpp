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

#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <nlohmann/json.hpp>
#include <silkworm/chain/block_chain.hpp>
#include <silkworm/chain/difficulty.hpp>
#include <silkworm/common/util.hpp>
#include <silkworm/execution/processor.hpp>
#include <silkworm/rlp/decode.hpp>
#include <silkworm/state/intra_block_state.hpp>
#include <silkworm/types/block.hpp>
#include <string>
#include <string_view>

// See https://ethereum-tests.readthedocs.io

using namespace silkworm;

namespace fs = std::filesystem;

static const fs::path kRootDir{SILKWORM_CONSENSUS_TEST_DIR};

static const fs::path kDifficultyDir{kRootDir / "BasicTests"};

static const fs::path kBlockchainDir{kRootDir / "BlockchainTests"};

static const fs::path kTransactionDir{kRootDir / "TransactionTests"};

static const std::set<fs::path> kSlowTests{
    kBlockchainDir / "GeneralStateTests" / "stTimeConsuming",
};

// TODO[Issue #23] make the failing tests work
static const std::set<fs::path> kFailingTests{
    kBlockchainDir / "InvalidBlocks",

    // Expected: "UnknownParent"
    kBlockchainDir / "TransitionTests" / "bcFrontierToHomestead" / "HomesteadOverrideFrontier.json",

    // Nonce >= 2^64 is not supported
    kTransactionDir / "ttNonce" / "TransactionWithHighNonce256.json",

    // Gas limit >= 2^64 is not supported
    kTransactionDir / "ttGasLimit" / "TransactionWithGasLimitxPriceOverflow.json",
};

constexpr size_t kColumnWidth{80};

static const std::map<std::string, silkworm::ChainConfig> kNetworkConfig{
    {"Frontier",
     {
         1,  // chain_id
     }},
    {"Homestead",
     {
         1,  // chain_id
         0,  // homestead_block
     }},
    {"EIP150",
     {
         1,  // chain_id
         0,  // homestead_block
         0,  // tangerine_whistle_block
     }},
    {"EIP158",
     {
         1,  // chain_id
         0,  // homestead_block
         0,  // tangerine_whistle_block
         0,  // spurious_dragon_block
     }},
    {"Byzantium",
     {
         1,  // chain_id
         0,  // homestead_block
         0,  // tangerine_whistle_block
         0,  // spurious_dragon_block
         0,  // byzantium_block
     }},
    {"Constantinople",
     {
         1,  // chain_id
         0,  // homestead_block
         0,  // tangerine_whistle_block
         0,  // spurious_dragon_block
         0,  // byzantium_block
         0,  // constantinople_block
     }},
    {"ConstantinopleFix",
     {
         1,  // chain_id
         0,  // homestead_block
         0,  // tangerine_whistle_block
         0,  // spurious_dragon_block
         0,  // byzantium_block
         0,  // constantinople_block
         0,  // petersburg_block
     }},
    {"Istanbul",
     {
         1,  // chain_id
         0,  // homestead_block
         0,  // tangerine_whistle_block
         0,  // spurious_dragon_block
         0,  // byzantium_block
         0,  // constantinople_block
         0,  // petersburg_block
         0,  // istanbul_block
     }},
    {"FrontierToHomesteadAt5",
     {
         1,  // chain_id
         5,  // homestead_block
     }},
    {"HomesteadToEIP150At5",
     {
         1,  // chain_id
         0,  // homestead_block
         5,  // tangerine_whistle_block
     }},
    {"HomesteadToDaoAt5",
     {
         1,   // chain_id
         0,   // homestead_block
         {},  // tangerine_whistle_block
         {},  // spurious_dragon_block
         {},  // byzantium_block
         {},  // constantinople_block
         {},  // petersburg_block
         {},  // istanbul_block
         {},  // muir_glacier_block
         5,   // dao_block
     }},
    {"EIP158ToByzantiumAt5",
     {
         1,  // chain_id
         0,  // homestead_block
         0,  // tangerine_whistle_block
         0,  // spurious_dragon_block
         5,  // byzantium_block
     }},
    {"ByzantiumToConstantinopleFixAt5",
     {
         1,  // chain_id
         0,  // homestead_block
         0,  // tangerine_whistle_block
         0,  // spurious_dragon_block
         0,  // byzantium_block
         5,  // constantinople_block
         5,  // petersburg_block
     }},
};

// https://ethereum-tests.readthedocs.io/en/latest/test_types/blockchain_tests.html#pre-prestate-section
IntraBlockState pre_state(const nlohmann::json& pre) {
  IntraBlockState state{nullptr};

  for (const auto& entry : pre.items()) {
    evmc::address address{to_address(from_hex(entry.key()))};
    const nlohmann::json& account{entry.value()};
    Bytes balance{from_hex(account["balance"].get<std::string>())};
    state.set_balance(address, rlp::read_uint256(balance, /*allow_leading_zeros=*/true));
    state.set_code(address, from_hex(account["code"].get<std::string>()));
    Bytes nonce{from_hex(account["nonce"].get<std::string>())};
    state.set_nonce(address, rlp::read_uint64(nonce, /*allow_leading_zeros=*/true));
    for (const auto& storage : account["storage"].items()) {
      Bytes key{from_hex(storage.key())};
      Bytes value{from_hex(storage.value().get<std::string>())};
      state.set_storage(address, to_bytes32(key), to_bytes32(value));
    }
  }

  state.finalize_transaction();
  return state;
}

enum Status { kPassed, kFailed, kSkipped };

Status run_block(const nlohmann::json& b, BlockChain& chain, IntraBlockState& state) {
  bool invalid{b.contains("expectException")};

  Block block;
  ByteView view{};

  try {
    Bytes rlp{from_hex(b["rlp"].get<std::string>())};
    view = rlp;
    rlp::decode(view, block);
  } catch (const std::exception& e) {
    if (invalid) {
      return kPassed;
    }
    std::cout << e.what() << "\n";
    return kFailed;
  }

  if (!view.empty()) {
    if (invalid) {
      return kPassed;
    }
    std::cout << "Extra RLP input\n";
    return kFailed;
  }

  try {
    chain.insert_block(block);
  } catch (const std::exception& e) {
    std::cout << e.what() << "\n";
    return kSkipped;
  }

  uint64_t block_number{block.header.number};
  bool homestead{chain.config.has_homestead(block_number)};
  bool spurious_dragon{chain.config.has_spurious_dragon(block_number)};

  for (Transaction& txn : block.transactions) {
    if (spurious_dragon) {
      txn.recover_sender(homestead, chain.config.chain_id);
    } else {
      txn.recover_sender(homestead, {});
    }
  }

  ExecutionProcessor processor{chain, block, state};
  try {
    processor.execute_block();
  } catch (const ValidationError& e) {
    if (invalid) {
      return kPassed;
    }
    std::cout << e.what() << "\n";
    return kFailed;
  }

  if (invalid) {
    std::cout << "Invalid block executed successfully\n";
    std::cout << "Expected: " << b["expectException"] << "\n";
    return kFailed;
  }

  return kPassed;
}

bool post_check(const IntraBlockState& state, const nlohmann::json& expected) {
  for (const auto& entry : expected.items()) {
    evmc::address address{to_address(from_hex(entry.key()))};
    const nlohmann::json& account{entry.value()};

    Bytes expected_balance{from_hex(account["balance"].get<std::string>())};
    intx::uint256 actual_balance{state.get_balance(address)};
    if (actual_balance != rlp::read_uint256(expected_balance, /*allow_leading_zeros=*/true)) {
      std::cout << "Balance mismatch for " << entry.key() << ":\n";
      std::cout << to_string(actual_balance, 16) << " ≠ " << account["balance"] << "\n";
      return false;
    }

    Bytes nonce_str{from_hex(account["nonce"].get<std::string>())};
    uint64_t expected_nonce{rlp::read_uint64(nonce_str, /*allow_leading_zeros=*/true)};
    uint64_t actual_nonce{state.get_nonce(address)};
    if (actual_nonce != expected_nonce) {
      std::cout << "Nonce mismatch for " << entry.key() << ":\n";
      std::cout << actual_nonce << " ≠ " << expected_nonce << "\n";
      return false;
    }

    auto expected_code{account["code"].get<std::string>()};
    Bytes actual_code{state.get_code(address)};
    if (actual_code != from_hex(expected_code)) {
      std::cout << "Code mismatch for " << entry.key() << ":\n";
      std::cout << to_hex(actual_code) << " ≠ " << expected_code << "\n";
      return false;
    }

    for (const auto& storage : account["storage"].items()) {
      Bytes key{from_hex(storage.key())};
      Bytes expected_value{from_hex(storage.value().get<std::string>())};
      evmc::bytes32 actual_value{state.get_current_storage(address, to_bytes32(key))};
      if (actual_value != to_bytes32(expected_value)) {
        std::cout << "Storage mismatch for " << entry.key() << " at " << storage.key() << ":\n";
        std::cout << to_hex(actual_value) << " ≠ " << to_hex(expected_value) << "\n";
        return false;
      }
    }
  }

  return true;
}

// https://ethereum-tests.readthedocs.io/en/latest/test_types/blockchain_tests.html
Status blockchain_test(const nlohmann::json& j) {
  if (!j.contains("postState")) {
    std::cout << "postStateHash is not supported\n";
    return kSkipped;
  }

  Bytes genesis_rlp{from_hex(j["genesisRLP"].get<std::string>())};
  ByteView genesis_view{genesis_rlp};
  Block genesis_block;
  rlp::decode(genesis_view, genesis_block);

  BlockChain chain{nullptr};
  std::string network{j["network"].get<std::string>()};
  chain.config = kNetworkConfig.at(network);
  chain.insert_block(genesis_block);

  IntraBlockState state{pre_state(j["pre"])};

  for (const auto& b : j["blocks"]) {
    Status status{run_block(b, chain, state)};
    if (status != kPassed) {
      return status;
    }
  }

  if (post_check(state, j["postState"])) {
    return kPassed;
  } else {
    return kFailed;
  }
}

static void print_test_status(std::string_view key, Status status) {
  std::cout << key << " ";
  for (size_t i{key.length() + 1}; i < kColumnWidth; ++i) {
    std::cout << '.';
  }
  switch (status) {
    case kPassed:
      std::cout << "\033[0;32m  Passed\033[0m\n";
      break;
    case kFailed:
      std::cout << "\033[1;31m  Failed\033[0m\n";
      break;
    case kSkipped:
      std::cout << " Skipped\n";
      break;
  }
}

struct RunResults {
  size_t passed{0};
  size_t failed{0};
  size_t skipped{0};

  RunResults& operator+=(const RunResults& rhs) {
    passed += rhs.passed;
    failed += rhs.failed;
    skipped += rhs.skipped;
    return *this;
  }

  void add(Status status) {
    switch (status) {
      case kPassed:
        ++passed;
        break;
      case kFailed:
        ++failed;
        break;
      case kSkipped:
        ++skipped;
        break;
    }
  }
};

[[nodiscard]] RunResults run_test_file(const fs::path& file_path,
                                       Status (*runner)(const nlohmann::json&)) {
  std::ifstream in{file_path};
  nlohmann::json json;
  in >> json;

  RunResults res{};

  for (const auto& test : json.items()) {
    Status status{runner(test.value())};
    res.add(status);
    if (status != kPassed) {
      print_test_status(test.key(), status);
    }
  }

  return res;
}

// https://ethereum-tests.readthedocs.io/en/latest/test_types/transaction_tests.html
Status transaction_test(const nlohmann::json& j) {
  Transaction txn;
  bool decoded{false};
  try {
    Bytes rlp{from_hex(j["rlp"].get<std::string>())};
    ByteView view{rlp};
    rlp::decode(view, txn);
    decoded = view.empty();
  } catch (const std::exception&) {
  }

  for (const auto& entry : j.items()) {
    if (entry.key() == "rlp" || entry.key() == "_info") {
      continue;
    }

    bool valid{entry.value().contains("sender")};

    if (!decoded) {
      if (valid) {
        std::cout << "Failed to decode valid transaction\n";
        return kFailed;
      } else {
        continue;
      }
    }

    ChainConfig config{kNetworkConfig.at(entry.key())};
    bool homestead{config.has_homestead(0)};
    bool spurious_dragon{config.has_spurious_dragon(0)};
    bool istanbul{config.has_istanbul(0)};

    intx::uint128 g0{intrinsic_gas(txn, homestead, istanbul)};
    if (g0 > txn.gas_limit) {
      if (valid) {
        std::cout << "g0 > gas_limit for valid transaction\n";
        return kFailed;
      } else {
        continue;
      }
    }

    if (spurious_dragon) {
      txn.recover_sender(homestead, config.chain_id);
    } else {
      txn.recover_sender(homestead, {});
    }

    if (valid && !txn.from.has_value()) {
      std::cout << "Failed to recover sender\n";
      return kFailed;
    }

    if (!valid && txn.from.has_value()) {
      std::cout << entry.key() << "\n";
      std::cout << "Sender recovered for invalid transaction\n";
      return kFailed;
    }

    if (!valid) {
      continue;
    }

    std::string expected{entry.value()["sender"].get<std::string>()};
    if (to_hex(*txn.from) != expected) {
      std::cout << "Sender mismatch for " << entry.key() << ":\n";
      std::cout << to_hex(*txn.from) << " ≠ " << expected << "\n";
      return kFailed;
    }
  }

  return kPassed;
}

// https://ethereum-tests.readthedocs.io/en/latest/test_types/difficulty_tests.html
Status difficulty_test(const nlohmann::json& j) {
  auto parent_timestamp{std::stoll(j["parentTimestamp"].get<std::string>(), 0, 0)};
  auto parent_difficulty{
      intx::from_string<intx::uint256>(j["parentDifficulty"].get<std::string>())};
  auto current_timestamp{std::stoll(j["currentTimestamp"].get<std::string>(), 0, 0)};
  auto block_number{std::stoll(j["currentBlockNumber"].get<std::string>(), 0, 0)};
  auto current_difficulty{
      intx::from_string<intx::uint256>(j["currentDifficulty"].get<std::string>())};

  auto parent_uncles{j["parentUncles"].get<std::string>()};
  bool parent_has_uncles{parent_uncles !=
                         "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"};

  intx::uint256 calculated_difficulty{canonical_difficulty(block_number, current_timestamp,
                                                           parent_difficulty, parent_timestamp,
                                                           parent_has_uncles, kEthMainnetConfig)};

  if (calculated_difficulty == current_difficulty) {
    return kPassed;
  } else {
    std::cout << hex(calculated_difficulty) << " ≠ " << hex(current_difficulty) << "\n";
    return kFailed;
  }
}

int main() {
  RunResults res{};

  res += run_test_file(kDifficultyDir / "difficulty.json", difficulty_test);
  res += run_test_file(kDifficultyDir / "difficultyMainNetwork.json", difficulty_test);

  for (auto i = fs::recursive_directory_iterator(kBlockchainDir);
       i != fs::recursive_directory_iterator{}; ++i) {
    if (kSlowTests.count(*i) || kFailingTests.count(*i)) {
      i.disable_recursion_pending();
    } else if (i->is_regular_file()) {
      res += run_test_file(*i, blockchain_test);
    }
  }

  for (auto i = fs::recursive_directory_iterator(kTransactionDir);
       i != fs::recursive_directory_iterator{}; ++i) {
    if (kFailingTests.count(*i)) {
      i.disable_recursion_pending();
    } else if (i->is_regular_file()) {
      res += run_test_file(*i, transaction_test);
    }
  }

  std::cout << "\033[0;32m" << res.passed << " tests passed\033[0m, ";
  if (res.failed) {
    std::cout << "\033[1;31m";
  }
  std::cout << res.failed << " failed";
  if (res.failed) {
    std::cout << "\033[0m";
  }
  std::cout << ", " << res.skipped << " skipped\n";

  return res.failed;
}
