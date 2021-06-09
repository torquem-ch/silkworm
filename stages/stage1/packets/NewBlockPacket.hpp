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

#ifndef SILKWORM_NEWBLOCKPACKET_HPP
#define SILKWORM_NEWBLOCKPACKET_HPP

#include "stages/stage1/Types.hpp"

namespace silkworm {

    struct NewBlockPacket {
        Block block;
        BigInt td; // total difficulty
    };

namespace rlp {
    inline void encode(Bytes& to, const NewBlockPacket& from) noexcept {
        rlp::Header rlp_head{true,
                             rlp::length(from.block) + rlp::length(from.td)};

        rlp::encode_header(to, rlp_head);

        rlp::encode(to, from.block);
        rlp::encode(to, from.td);
    }

    inline size_t length(const NewBlockPacket& from) noexcept {
        rlp::Header rlp_head{true,
                             rlp::length(from.block) + rlp::length(from.td)};

        size_t rlp_head_len = rlp::length_of_length(rlp_head.payload_length);
        return rlp_head_len + rlp_head.payload_length;
    }

    inline rlp::DecodingResult decode(ByteView& from, NewBlockPacket& to) noexcept {

        auto [rlp_head, err]{decode_header(from)};
        if (err != DecodingResult::kOk) {
            return err;
        }
        if (!rlp_head.list) {
            return DecodingResult::kUnexpectedString;
        }

        uint64_t leftover{from.length() - rlp_head.payload_length};

        if (DecodingResult err{rlp::decode(from, to.block)}; err != DecodingResult::kOk) {
            return err;
        }
        if (DecodingResult err{rlp::decode(from, to.td)}; err != DecodingResult::kOk) {
            return err;
        }

        return from.length() == leftover ? DecodingResult::kOk : DecodingResult::kListLengthMismatch;
    }

}

    inline std::ostream& operator<<(std::ostream& os, const NewBlockPacket& packet)
    {
        os << "block num " << packet.block.header.number;
        return os;
    }

}

#endif  // SILKWORM_NEWBLOCKPACKET_HPP
