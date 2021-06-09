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

#ifndef SILKWORM_NEWBLOCKHASHPACKET_HPP
#define SILKWORM_NEWBLOCKHASHPACKET_HPP

#include "stages/stage1/Types.hpp"

namespace silkworm {

    struct NewBlockHash {     // one particular block being announced
        Hash hash;        // hash of the block
        BlockNum number;  // number of the block
    };

    using NewBlockHashesPacket = std::vector<NewBlockHash>;

namespace rlp {

    inline void encode(Bytes& to, const NewBlockHash& from) noexcept {
        rlp::Header rlp_head{true,
                             rlp::length(from.hash) + rlp::length(from.number)};

        rlp::encode_header(to, rlp_head);

        rlp::encode(to, from.hash);
        rlp::encode(to, from.number);
    }

    inline size_t length(const NewBlockHash& from) noexcept {
        rlp::Header rlp_head{true,
                             rlp::length(from.hash) + rlp::length(from.number)};

        size_t rlp_head_len = rlp::length_of_length(rlp_head.payload_length);
        return rlp_head_len + rlp_head.payload_length;
    }

    inline rlp::DecodingResult decode(ByteView& from, NewBlockHash& to) noexcept {

        auto [rlp_head, err]{decode_header(from)};
        if (err != DecodingResult::kOk) {
            return err;
        }
        if (!rlp_head.list) {
            return DecodingResult::kUnexpectedString;
        }

        uint64_t leftover{from.length() - rlp_head.payload_length};

        if (DecodingResult err{rlp::decode(from, to.hash)}; err != DecodingResult::kOk) {
            return err;
        }
        if (DecodingResult err{rlp::decode(from, to.number)}; err != DecodingResult::kOk) {
            return err;
        }

        return from.length() == leftover ? DecodingResult::kOk : DecodingResult::kListLengthMismatch;
    }

    // size_t length(const NewBlockHashesPacket& from)           implemented by  rlp::length<T>(const std::vector<T>& v)
    // void encode(Bytes& to, const NewBlockHashesPacket& from)  implemented by  rlp::encode<T>(Bytes& to, const std::vector<T>& v)

    void encode(Bytes& to, const NewBlockHashesPacket& from);

    size_t length(const NewBlockHashesPacket& from);

    rlp::DecodingResult decode(ByteView& from, NewBlockHashesPacket& to) noexcept;

}

    inline std::ostream& operator<<(std::ostream& os, const NewBlockHashesPacket& packet)
    {
        if (packet.size() == 1)
            os << "block num " << packet[0].number /* << " hash " << to_hex(packet[0].hash) */;
        else
            os << packet.size() << " new block hashes/nums";
        return os;
    }

} // silkworm namespace

#endif  // SILKWORM_NEWBLOCKHASHPACKET_HPP
