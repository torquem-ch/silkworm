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

#ifndef SILKWORM_GETBLOCKBODIESPACKET_HPP
#define SILKWORM_GETBLOCKBODIESPACKET_HPP

#include "stages/stage1/Types.hpp"

namespace silkworm {

    using GetBlockBodiesPacket = std::vector<Hash>;

    struct GetBlockBodiesPacket66 { // eth/66 version
        uint64_t requestId;
        GetBlockBodiesPacket request;
    };

namespace rlp {

    // size_t length(const GetBlockBodiesPacket& from)           implemented by  rlp::length<T>(const std::vector<T>& v)
    // void encode(Bytes& to, const GetBlockBodiesPacket& from)  implemented by  rlp::encode(Bytes& to, const std::vector<T>& v)

    rlp::DecodingResult decode(ByteView& from, GetBlockBodiesPacket& to) noexcept;

    // ... length(const GetBlockBodiesPacket66& from)            implemented by template <Eth66Packet T> size_t length(const T& from)

    // ... encode(Bytes& to, const GetBlockBodiesPacket66& from) implemented by template <Eth66Packet T> void encode(Bytes& to, const T& from)

    // ... decode(ByteView& from, GetBlockBodiesPacket66& to)    implemented by template <Eth66Packet T> rlp::DecodingResult decode(ByteView& from, T& to)

} // rlp namespace

    inline std::ostream& operator<<(std::ostream& os, const GetBlockBodiesPacket66& packet)
    {
        os << " reqId="  << packet.requestId;

        if (packet.request.size() == 1)
            os << " hash=" << to_hex(packet.request[0]);
        else
            os << " hash=" << packet.request.size() << " block hashes";

        return os;
    }

} // silkworm namespace



#endif  // SILKWORM_GETBLOCKBODIESPACKET_HPP
