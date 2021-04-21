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

#ifndef SILKWORM_CHAIN_CONFIG_HPP_
#define SILKWORM_CHAIN_CONFIG_HPP_

#include <stdint.h>

#include <optional>

#include <nlohmann/json.hpp>

namespace silkworm {

struct ChainConfig {

    // https://eips.ethereum.org/EIPS/eip-155
    uint64_t chain_id{0};

    /*
    * Note for developers !
    * Adding/removing members here require to integrate Json() method
    * in config.cpp *AND* in silkworm/db/common/access_layer.cpp
    */

#define IMPLEMENT_HAS(NAME)                                                   \
    bool has_##NAME(uint64_t block_num) const noexcept {                      \
        return NAME##_block.has_value() && NAME##_block.value() <= block_num; \
    }

    // https://eips.ethereum.org/EIPS/eip-606
    std::optional<uint64_t> homestead_block;
    IMPLEMENT_HAS(homestead)

    // https://eips.ethereum.org/EIPS/eip-608
    // https://ecips.ethereumclassic.org/ECIPs/ecip-1015
    std::optional<uint64_t> tangerine_whistle_block;
    IMPLEMENT_HAS(tangerine_whistle);

    // TODO[ETC] EIP-160 was applied to ETC before the rest of Spurious Dragon; see
    // https://ecips.ethereumclassic.org/ECIPs/ecip-1066

    // https://eips.ethereum.org/EIPS/eip-607
    // https://ecips.ethereumclassic.org/ECIPs/ecip-1054
    std::optional<uint64_t> spurious_dragon_block;
    IMPLEMENT_HAS(spurious_dragon);

    // https://eips.ethereum.org/EIPS/eip-609
    // https://ecips.ethereumclassic.org/ECIPs/ecip-1054
    std::optional<uint64_t> byzantium_block;
    IMPLEMENT_HAS(byzantium);

    // https://eips.ethereum.org/EIPS/eip-1013
    // https://ecips.ethereumclassic.org/ECIPs/ecip-1056
    std::optional<uint64_t> constantinople_block;
    IMPLEMENT_HAS(constantinople);

    // https://eips.ethereum.org/EIPS/eip-1716
    // https://ecips.ethereumclassic.org/ECIPs/ecip-1056
    std::optional<uint64_t> petersburg_block;
    IMPLEMENT_HAS(petersburg);

    // https://eips.ethereum.org/EIPS/eip-1679
    // https://ecips.ethereumclassic.org/ECIPs/ecip-1088
    std::optional<uint64_t> istanbul_block;
    IMPLEMENT_HAS(istanbul);

    // https://eips.ethereum.org/EIPS/eip-2387
    std::optional<uint64_t> muir_glacier_block;
    IMPLEMENT_HAS(muir_glacier);

    // https://github.com/ethereum/eth1.0-specs/blob/master/network-upgrades/berlin.md
    std::optional<uint64_t> berlin_block;
    IMPLEMENT_HAS(berlin);

    // https://eips.ethereum.org/EIPS/eip-779
    std::optional<uint64_t> dao_block;
    IMPLEMENT_HAS(dao);

#undef IMPLEMENT_HAS

    nlohmann::json Json() const noexcept;
};

bool operator==(const ChainConfig& a, const ChainConfig& b);
std::ostream& operator<<(std::ostream& out, const ChainConfig& obj);

constexpr ChainConfig kMainnetConfig{
    1,  // chain_id

    1'150'000,   // homestead_block
    2'463'000,   // tangerine_whistle_block
    2'675'000,   // spurious_dragon_block
    4'370'000,   // byzantium_block
    7'280'000,   // constantinople_block
    7'280'000,   // petersburg_block
    9'069'000,   // istanbul_block
    9'200'000,   // muir_glacier_block
    12'244'000,  // berlin_block

    1'920'000,  // dao_block
};

constexpr ChainConfig kRopstenConfig{
    3,  // chain_id

    0,          // homestead_block
    0,          // tangerine_whistle_block
    10,         // spurious_dragon_block
    1'700'000,  // byzantium_block
    4'230'000,  // constantinople_block
    4'939'394,  // petersburg_block
    6'485'846,  // istanbul_block
    7'117'117,  // muir_glacier_block
    9'812'189,  // berlin_block
};

constexpr ChainConfig kRinkebyConfig{
    4,  // chain_id

    1,             // homestead_block
    2,             // tangerine_whistle_block
    3,             // spurious_dragon_block
    1'035'301,     // byzantium_block
    3'660'663,     // constantinople_block
    4'321'234,     // petersburg_block
    5'435'345,     // istanbul_block
    std::nullopt,  // muir_glacier_block
    8'290'928,     // berlin_block
};

constexpr ChainConfig kGoerliConfig{
    5,  // chain_id

    0,             // homestead_block
    0,             // tangerine_whistle_block
    0,             // spurious_dragon_block
    0,             // byzantium_block
    0,             // constantinople_block
    0,             // petersburg_block
    1'561'651,     // istanbul_block
    std::nullopt,  // muir_glacier_block
    4'460'644,     // berlin_block
};

// https://ecips.ethereumclassic.org/ECIPs/ecip-1066
constexpr ChainConfig kClassicMainnetConfig{
    61,  // chain_id

    1'150'000,   // homestead_block
    2'500'000,   // tangerine_whistle_block
    8'772'000,   // spurious_dragon_block
    8'772'000,   // byzantium_block
    9'573'000,   // constantinople_block
    9'573'000,   // petersburg_block
    10'500'839,  // istanbul_block
};

inline const ChainConfig* lookup_chain_config(uint64_t chain_id) noexcept {
    switch (chain_id) {
        case kMainnetConfig.chain_id:
            return &kMainnetConfig;
        case kRopstenConfig.chain_id:
            return &kRopstenConfig;
        case kRinkebyConfig.chain_id:
            return &kRinkebyConfig;
        case kGoerliConfig.chain_id:
            return &kGoerliConfig;
        case kClassicMainnetConfig.chain_id:
            return &kClassicMainnetConfig;
        default:
            return nullptr;
    }
}

}  // namespace silkworm

#endif  // SILKWORM_CHAIN_CONFIG_HPP_
