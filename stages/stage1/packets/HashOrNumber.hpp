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

#include "stages/stage1/Types.hpp"
#include "RLPError.hpp"

namespace silkworm {

    // HashOrNumber type def
    using HashOrNumber = std::variant<Hash, BlockNum>;

// HashOrNumber rlp encoding/decoding
namespace rlp {

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
        ByteView copy(from);    // a copy because we need only decode header and not consume it
        auto [h, err] = decode_header(copy); // so we can use full implementation of decode below
        if (err != DecodingResult::kOk)
            return err;
        if (h.list)
            return DecodingResult::kUnexpectedList;

        if (h.payload_length == 32) {
            Hash hash;
            err = rlp::decode(from, dynamic_cast<evmc::bytes32&>(hash));    // consume header
            to = hash;
        } else if (h.payload_length <= 8) {
            BlockNum number{};
            err = rlp::decode(from, number);    // consume header
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
