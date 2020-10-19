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

#include "receipt.hpp"

#include <catch2/catch.hpp>
#include <silkworm/common/util.hpp>

namespace silkworm {

TEST_CASE("CBOR encoding of receipts") {
    std::vector<Receipt> v{};
    std::vector<uint8_t> encoded{cbor_encode(v)};
    CHECK(to_hex(full_view(encoded)) == "f6");

    v.resize(2);

    v[0].success = false;
    v[0].cumulative_gas_used = 3338333;
    v[0].logs = {
        Log{
            0xea674fdde714fd979de3edf0f56aa9716b898ec8_address,
            {},
            from_hex("0x010043"),
        },
        Log{
            0x44fd3ab8381cc3d14afa7c4af7fd13cdc65026e1_address,
            {to_bytes32(from_hex("dead")), to_bytes32(from_hex("abba"))},
            from_hex("0xaabbff780043"),
        },
    };

    v[1].success = true;
    v[1].cumulative_gas_used = 12496336;
    v[1].logs = {};

    encoded = cbor_encode(v);

    CHECK(to_hex(full_view(encoded)) ==
          "8284f6001a0032f05d828354ea674fdde714fd979de3edf0f56aa9716b898ec88043010043835444fd3ab8381cc3d14afa7c4af7fd13"
          "cdc65026e1825820000000000000000000000000000000000000000000000000000000000000dead5820000000000000000000000000"
          "000000000000000000000000000000000000abba46aabbff78004384f6011a00beadd0f6");
}

}  // namespace silkworm
