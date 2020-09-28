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

#ifndef SILKWORM_EXECUTION_EVM_H_
#define SILKWORM_EXECUTION_EVM_H_

#include <stdint.h>

#include <evmc/evmc.hpp>
#include <intx/intx.hpp>
#include <silkworm/chain/block_chain.hpp>
#include <silkworm/state/intra_block_state.hpp>
#include <silkworm/types/block.hpp>
#include <stack>
#include <vector>

// TODO(Andrew) get rid of this when
// https://github.com/ethereum/evmc/pull/528
// is merged and released
enum evmc_status_code_extra { EVMC_BALANCE_TOO_LOW = 32 };

namespace silkworm {

struct CallResult {
    evmc_status_code status{EVMC_SUCCESS};
    uint64_t gas_left{0};
};

class EVM {
   public:
    EVM(const EVM&) = delete;
    EVM& operator=(const EVM&) = delete;

    EVM(const BlockChain& chain, const Block& block, IntraBlockState& state) noexcept;

    const Block& block() const noexcept { return block_; }

    const ChainConfig& config() const noexcept { return chain_.config; }

    IntraBlockState& state() noexcept { return state_; }

    CallResult execute(const Transaction& txn, uint64_t gas) noexcept;

   private:
    friend class EvmHost;

    evmc::result create(const evmc_message& message) noexcept;

    evmc::result call(const evmc_message& message) noexcept;

    evmc::result execute(const evmc_message& message, ByteView code, std::optional<evmc::bytes32> code_hash) noexcept;

    evmc_revision revision() const noexcept;

    uint8_t number_of_precompiles() const noexcept;
    bool is_precompiled(const evmc::address& contract) const noexcept;

    const BlockChain& chain_;
    const Block& block_;
    IntraBlockState& state_;
    const Transaction* txn_{nullptr};
    std::vector<evmc::bytes32> block_hashes_{};
    std::stack<evmc::address> address_stack_{};
};

class EvmHost : public evmc::Host {
   public:
    explicit EvmHost(EVM& evm) noexcept : evm_{evm} {}

    bool account_exists(const evmc::address& address) const noexcept override;

    evmc::bytes32 get_storage(const evmc::address& address, const evmc::bytes32& key) const noexcept override;

    evmc_storage_status set_storage(const evmc::address& address, const evmc::bytes32& key,
                                    const evmc::bytes32& value) noexcept override;

    evmc::uint256be get_balance(const evmc::address& address) const noexcept override;

    size_t get_code_size(const evmc::address& address) const noexcept override;

    evmc::bytes32 get_code_hash(const evmc::address& address) const noexcept override;

    size_t copy_code(const evmc::address& address, size_t code_offset, uint8_t* buffer_data,
                     size_t buffer_size) const noexcept override;

    void selfdestruct(const evmc::address& address, const evmc::address& beneficiary) noexcept override;

    evmc::result call(const evmc_message& message) noexcept override;

    evmc_tx_context get_tx_context() const noexcept override;

    evmc::bytes32 get_block_hash(int64_t block_number) const noexcept override;

    void emit_log(const evmc::address& address, const uint8_t* data, size_t data_size, const evmc::bytes32 topics[],
                  size_t num_topics) noexcept override;

   private:
    EVM& evm_;
};
}  // namespace silkworm

#endif  // SILKWORM_EXECUTION_EVM_H_
