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

// types
#include "stages/stage1/Types.hpp"
#include "BlockBodiesPacket.hpp"
#include "BlockHeadersPacket.hpp"
#include "GetBlockBodiesPacket.hpp"
#include "NewBlockHashesPacket.hpp"

// generic implementations (must follow types)
#include <silkworm/rlp/decode.hpp>
#include "RLPVectorCoding.hpp"

// specific implementations
namespace silkworm::rlp {

    rlp::DecodingResult decode(ByteView& from, Hash& to) noexcept {
        return rlp::decode(from, dynamic_cast<evmc::bytes32&>(to));
    }

    rlp::DecodingResult decode(ByteView& from, BlockBodiesPacket& to) noexcept {
        return rlp::decode_vec(from, to);  // decode_vec
    }

    rlp::DecodingResult decode(ByteView& from, BlockHeadersPacket& to) noexcept {
        return rlp::decode_vec(from, to); //decode_vec
    }

    rlp::DecodingResult decode(ByteView& from, GetBlockBodiesPacket& to) noexcept {
        return rlp::decode_vec(from, to); //decode_vec
    }

    rlp::DecodingResult decode(ByteView& from, NewBlockHashesPacket& to) noexcept {
        return rlp::decode_vec(from, to); // decode_vec
    }
}

#include "RLPEth66PacketCoding.hpp"
