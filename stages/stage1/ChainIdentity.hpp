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

#ifndef SILKWORM_CHAINIDENTITY_HPP
#define SILKWORM_CHAINIDENTITY_HPP

#include <silkworm/chain/config.hpp>
#include "Types.hpp"

namespace silkworm {

// EIP-2124 based chain identity scheme (networkId + genesis + forks)
struct ChainIdentity {

    std::string name;
    ChainConfig chain;
    Hash genesis_hash;

    std::vector<BlockNum> distinct_fork_numbers(); // helper method

    static ChainIdentity mainnet;
    static ChainIdentity goerli;
};

}
#endif  // SILKWORM_CHAINIDENTITY_HPP
