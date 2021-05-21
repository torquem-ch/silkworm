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
#include "ChainIdentity.hpp"

namespace silkworm {


std::vector<BlockNum> ChainIdentity::distinct_fork_numbers() {
    std::vector<BlockNum> forks;

    for(std::optional<uint64_t> bn: chain.fork_blocks) {
        if (bn && *bn != 0)
            forks.push_back(*bn);
    }
    if (chain.dao_block)
        forks.push_back(*chain.dao_block);
    if (chain.muir_glacier_block)
        forks.push_back(*chain.muir_glacier_block);

    sort(forks.begin(), forks.end());                             // block list must be ordered
    forks.erase(unique(forks.begin(), forks.end()), forks.end()); // do not repeat block if 2 forks overlap

    return forks;
}

static ChainIdentity mainnet_identity() {
    ChainIdentity id;

    id.name = "mainnet";
    id.chain = kMainnetConfig;
    id.genesis_hash = Hash::from_hex("d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"); // mainnet genesis hash in hex

    return id;
}

static ChainIdentity goerli_identity() {
    ChainIdentity id;

    id.name = "goerli";
    id.chain = kGoerliConfig;
    id.genesis_hash = Hash::from_hex("bf7e331f7f7c1dd2e05159666b3bf8bc7a8a3a9eb1d518969eab529dd9b88c1a"); // goerli genesis hash in hex

    return id;
}

ChainIdentity ChainIdentity::mainnet = mainnet_identity();
ChainIdentity ChainIdentity::goerli = goerli_identity();

}