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

#ifndef SILKWORM_HASHORNUMBER_HPP
#define SILKWORM_HASHORNUMBER_HPP

#include <variant>

#include <silkworm/rlp/decode.hpp>
#include <silkworm/rlp/encode.hpp>

#include "Types.hpp"

namespace silkworm {

// HashOrNumber type def
using HashOrNumber = std::variant<Hash, BlockNum>;

// HashOrNumber rlp encoding/decoding
namespace rlp {

    class rlp_error : public std::runtime_error {
      public:
        rlp_error() : std::runtime_error("rlp encoding/decoding error") {}
        rlp_error(const std::string& description) : std::runtime_error(description) {}
    };

    inline void encode(Bytes& to, const HashOrNumber& from) {
        if (std::holds_alternative<Hash>(from))
            rlp::encode(to, std::get<Hash>(from));
        else
            rlp::encode(to, std::get<BlockNum>(from));
    }

    inline size_t length(const HashOrNumber& from) {
        if (std::holds_alternative<Hash>(from))
            return rlp::length(std::get<Hash>(from));
        else
            return rlp::length(std::get<BlockNum>(from));
    }

    inline DecodingResult decode(ByteView& from, HashOrNumber& to) noexcept {
        ByteView copy(from);  // to decode but not consume
        auto [h, err] = decode_header(copy);
        if (err != DecodingResult::kOk) {
            return err;
        }

        // uint8_t payload_length = from[0] - 0x80; // in the simple cases that we need here

        if (h.payload_length == 32) {
            Hash hash;
            err = rlp::decode(from, dynamic_cast<evmc::bytes32&>(hash));
            to = hash;
        } else if (h.payload_length <= 8) {
            BlockNum number;
            err = rlp::decode(from, number);
            to = number;
        } else {
            err = DecodingResult::kUnexpectedLength;
        }
        return err;
    }

}

    inline std::ostream& operator<<(std::ostream& os, const HashOrNumber& packet) {
        if (std::holds_alternative<Hash>(packet))
            os << std::get<Hash>(packet);
        else
            os << std::get<BlockNum>(packet);
        return os;
    }

}
#endif  // SILKWORM_HASHORNUMBER_HPP
