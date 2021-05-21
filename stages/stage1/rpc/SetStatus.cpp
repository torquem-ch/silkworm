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

#include "SetStatus.hpp"

namespace silkworm::rpc {

SetStatus::SetStatus(ChainConfig chain, Hash genesis, std::vector<BlockNum> hard_forks, Hash best_hash, BigInt total_difficulty):
    AsyncUnaryCall("SetStatus", &sentry::Sentry::Stub::PrepareAsyncSetStatus, {})
{
    request_.set_network_id(chain.chain_id);

    request_.set_allocated_total_difficulty(to_H256(total_difficulty).release()); // remove trailing zeros???

    request_.set_allocated_best_hash(to_H256(best_hash).release());

    auto* forks = new sentry::Forks{};
    forks->set_allocated_genesis(to_H256(genesis).release());
    for(uint64_t block: hard_forks)
        forks->add_forks(block);
    request_.set_allocated_fork_data(forks); // take ownership
}

}
