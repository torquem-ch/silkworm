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

#include <catch2/catch.hpp>
#include "TypesForGrpc.hpp"

namespace silkworm {

TEST_CASE("H256/512 to/from conversions") {
    using namespace std;

    SECTION( "H256 to/from Hash" ) {
        Hash orig_hash = Hash::from_hex("d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3");
        Hash transf_hash = hash_from_H256(*to_H256(orig_hash));

        REQUIRE(orig_hash == transf_hash);
    }

    SECTION( "H256 to/from number" ) {
        intx::uint256 orig_big{intx::uint128(UINT64_MAX, 67890), intx::uint128(12345, UINT64_MAX)};
        intx::uint256 transf_big = uint256_from_H256(*to_H256(orig_big));

        REQUIRE(orig_big == transf_big);
    }

    for(auto len: {64,64,64,64,64,60,70}) {
        SECTION( "H512 to/from string, len="+to_string(len) ) {
            string orig_string(len, 0);
            generate_n(orig_string.begin(), len, [] { return static_cast<char>(rand() % 255); });

            string transf_string = string_from_H512(*to_H512(orig_string));

            orig_string.resize(64,0);   // transf_string is always of 64 bytes with trailing zeros if needed
            REQUIRE(orig_string == transf_string);
        }
    }
}

}