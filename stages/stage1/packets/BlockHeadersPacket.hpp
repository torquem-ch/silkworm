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

#ifndef SILKWORM_BLOCKHEADERSPACKET_HPP
#define SILKWORM_BLOCKHEADERSPACKET_HPP

#include "stages/stage1/Types.hpp"
#include <algorithm>

namespace silkworm {

    using BlockHeadersPacket = std::vector<BlockHeader>;

    struct BlockHeadersPacket66 {  // eth/66 version
        uint64_t requestId;
        BlockHeadersPacket request;
    };

namespace rlp {

    // size_t length(const BlockHeadersPacket& from)           implemented by  rlp::length<T>(const std::vector<T>& v)

    // void encode(Bytes& to, const BlockHeadersPacket& from)  implemented by  rlp::encode(Bytes& to, const std::vector<T>& v)

    rlp::DecodingResult decode(ByteView& from, BlockHeadersPacket& to) noexcept;

    // ... length(const BlockHeadersPacket66& from)            implemented by template <Eth66Packet T> size_t length(const T& from)

    // ... encode(Bytes& to, const BlockHeadersPacket66& from) implemented by template <Eth66Packet T> void encode(Bytes& to, const T& from)

    // ... decode(ByteView& from, BlockHeadersPacket66& to)    implemented by template <Eth66Packet T> rlp::DecodingResult decode(ByteView& from, T& to)

}

    inline std::ostream& operator<<(std::ostream& os, const BlockHeadersPacket66& packet)
    {
        os << "reqId=" << packet.requestId;
        os << " headers(bn)=";

        const size_t max_display = 3;
        for(size_t i = 0; i < std::min(packet.request.size(), max_display); i++) {
            os << packet.request[i].number << ",";
        }
        if (packet.request.size() > max_display)
            os << "...";

        return os;
    }

} // silkworm namespace

#include "RLPEth66PacketCoding.hpp"

#endif  // SILKWORM_BLOCKHEADERSPACKET_HPP
