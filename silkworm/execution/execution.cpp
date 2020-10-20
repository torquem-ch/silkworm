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

#include "execution.hpp"

#include <cassert>
#include <silkworm/db/access_layer.hpp>
#include <silkworm/trie/vector_root.hpp>
#include <stdexcept>

#include "processor.hpp"

namespace silkworm {

std::vector<Receipt> execute_block(BlockWithHash& bh, db::Buffer& buffer, const ChainConfig& config) {
    assert(buffer.transaction());
    lmdb::Transaction& txn{*buffer.transaction()};

    const BlockHeader& header{bh.block.header};
    uint64_t block_num{header.number};

    std::vector<evmc::address> senders{db::read_senders(txn, block_num, bh.hash)};
    if (senders.size() != bh.block.transactions.size()) {
        throw std::runtime_error("missing or incorrect senders");
    }
    for (size_t i{0}; i < senders.size(); ++i) {
        bh.block.transactions[i].from = senders[i];
    }

    IntraBlockState state{buffer};
    ExecutionProcessor processor{bh.block, state, config};

    std::vector<Receipt> receipts{processor.execute_block()};

    uint64_t gas_used{0};
    if (!receipts.empty()) {
        gas_used = receipts.back().cumulative_gas_used;
    }

    if (gas_used != header.gas_used) {
        throw ValidationError("gas mismatch for block " + std::to_string(block_num));
    }

    if (config.has_byzantium(block_num)) {
        evmc::bytes32 receipt_root{trie::root_hash(receipts)};
        if (receipt_root != header.receipts_root) {
            throw ValidationError("receipt root mismatch for block " + std::to_string(block_num));
        }
    }

    return receipts;
}

}  // namespace silkworm
