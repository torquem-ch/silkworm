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

#ifndef SILKWORM_GETBLOCKHEADERSPACKET_HPP
#define SILKWORM_GETBLOCKHEADERSPACKET_HPP

#include "HashOrNumber.hpp"

namespace silkworm {

    struct GetBlockHeadersPacket {
        HashOrNumber origin;  // Block hash or block number from which to retrieve headers
        uint64_t amount;      // Maximum number of headers to retrieve
        uint64_t skip;        // Blocks to skip between consecutive headers
        bool reverse;         // Query direction (false = rising towards latest, true = falling towards genesis)
    };

    struct GetBlockHeadersPacket66 { // eth/66 version
        uint64_t requestId;
        GetBlockHeadersPacket request;
    };

namespace rlp {

    inline void encode(Bytes& to, const GetBlockHeadersPacket& from) noexcept {
        rlp::Header rlp_head{true, 0};

        rlp_head.payload_length += rlp::length(from.origin);
        rlp_head.payload_length += rlp::length(from.amount);
        rlp_head.payload_length += rlp::length(from.skip);
        rlp_head.payload_length += rlp::length(from.reverse);

        rlp::encode_header(to, rlp_head);

        rlp::encode(to, from.origin);
        rlp::encode(to, from.amount);
        rlp::encode(to, from.skip);
        rlp::encode(to, from.reverse);
    }

    inline size_t length(const GetBlockHeadersPacket& from) noexcept {
        rlp::Header rlp_head{true, 0};

        rlp_head.payload_length += rlp::length(from.origin);
        rlp_head.payload_length += rlp::length(from.amount);
        rlp_head.payload_length += rlp::length(from.skip);
        rlp_head.payload_length += rlp::length(from.reverse);

        size_t rlp_head_len = rlp::length_of_length(rlp_head.payload_length);

        return rlp_head_len + rlp_head.payload_length;
    }

    inline rlp::DecodingResult decode(ByteView& from, GetBlockHeadersPacket& to) noexcept {
        using namespace rlp;

        auto [rlp_head, err]{decode_header(from)};
        if (err != DecodingResult::kOk) {
            return err;
        }
        if (!rlp_head.list) {
            return DecodingResult::kUnexpectedString;
        }

        uint64_t leftover{from.length() - rlp_head.payload_length};

        if (DecodingResult err{rlp::decode(from, to.origin)}; err != DecodingResult::kOk) {
            return err;
        }
        if (DecodingResult err{rlp::decode(from, to.amount)}; err != DecodingResult::kOk) {
            return err;
        }
        if (DecodingResult err{rlp::decode(from, to.skip)}; err != DecodingResult::kOk) {
            return err;
        }
        if (DecodingResult err{rlp::decode(from, to.reverse)}; err != DecodingResult::kOk) {
            return err;
        }

        return from.length() == leftover ? DecodingResult::kOk : DecodingResult::kListLengthMismatch;
    }

    // ... length(const GetBlockHeadersPacket66& from)            implemented by template <Eth66Packet T> size_t length(const T& from)

    // ... encode(Bytes& to, const GetBlockHeadersPacket66& from) implemented by template <Eth66Packet T> void encode(Bytes& to, const T& from)

    // ... decode(ByteView& from, GetBlockHeadersPacket66& to)    implemented by template <Eth66Packet T> rlp::DecodingResult decode(ByteView& from, T& to)

} // rlp namespace

    inline std::ostream& operator<<(std::ostream& os, const GetBlockHeadersPacket66& packet)
    {
        os <<   "reqId="  << packet.requestId
           << " origin=" << packet.request.origin
           << " amount=" << packet.request.amount
           << " skip="   << packet.request.skip
           << " reverse="<< packet.request.reverse;
        return os;
    }

} // silkworm namespace



#endif  // SILKWORM_GETBLOCKHEADERSPACKET_HPP
