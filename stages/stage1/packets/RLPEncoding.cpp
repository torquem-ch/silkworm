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
#include <silkworm/rlp/encode.hpp>
#include "RLPEth66PacketCoding.hpp"
#include "RLPVectorCoding.hpp"

namespace silkworm::rlp {

    void encode(Bytes& to, const Hash& h) {
        rlp::encode(to, dynamic_cast<const evmc::bytes32&>(h));
    }

    void encode(Bytes& to, const NewBlockHashesPacket& from) {
        rlp::encode_vec(to, from);
    }

    size_t length(const NewBlockHashesPacket& from) {
        return rlp::length_vec(from);
    }

}

