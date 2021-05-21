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
#include "NewBlockHashesPacket.hpp"

namespace silkworm {

// TESTs related to NewBlockHashesPacket encoding/decoding

/*
input:  e6e5a0eb2c33963824bf97d01cff8a65f00dc402fbf64f473cb4778a547ac08cebc35483bd8410
decoded:
        e6 = 26 bytes -> list
         |--  e5 = 25 bytes -> list
             |-- a0 = 20 bytes -> string
                  |-- eb2c33963824bf97d01cff8a65f00dc402fbf64f473cb4778a547ac08cebc354
             |-- 83 = 3 bytes -> string
                  |-- bd8410 (= decimal 12.420.112)
 */
TEST_CASE("NewBlockHashesPacket decoding") {
    using namespace std;

    optional<Bytes> encoded = from_hex("e6e5a0eb2c33963824bf97d01cff8a65f00dc402fbf64f473cb4778a547ac08cebc35483bd8410");
    REQUIRE(encoded.has_value());

    NewBlockHashesPacket packet;

    ByteView encoded_view = encoded.value();
    rlp::DecodingResult result = rlp::decode(encoded_view, packet);

    REQUIRE(result == rlp::DecodingResult::kOk);
    REQUIRE(packet.size() == 1);
    REQUIRE(packet[0].hash == Hash::from_hex("eb2c33963824bf97d01cff8a65f00dc402fbf64f473cb4778a547ac08cebc354"));
    REQUIRE(packet[0].number == 12'420'112);
}

TEST_CASE("NewBlockHashesPacket encoding") {
    using namespace std;

    NewBlockHashesPacket packet;

    NewBlock newBlock;
    newBlock.hash = Hash::from_hex("eb2c33963824bf97d01cff8a65f00dc402fbf64f473cb4778a547ac08cebc354");
    newBlock.number = 12'420'112;
    packet.push_back(newBlock);

    Bytes encoded;
    rlp::encode(encoded, packet);

    REQUIRE(to_hex(encoded) == "e6e5a0eb2c33963824bf97d01cff8a65f00dc402fbf64f473cb4778a547ac08cebc35483bd8410");
}

}