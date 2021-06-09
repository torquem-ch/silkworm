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
#ifndef SILKWORM_TYPES_HPP
#define SILKWORM_TYPES_HPP

#include <silkworm/common/util.hpp>
#include <silkworm/types/transaction.hpp>
#include <silkworm/rlp/decode.hpp>
#include <silkworm/rlp/encode.hpp>
#include <silkworm/types/block.hpp>
#include <boost/endian/conversion.hpp>
#include <iomanip>
#include <chrono>

namespace silkworm {

class Hash : public evmc::bytes32 {
  public:
    using evmc::bytes32::bytes32;

    Hash() {}
    Hash(ByteView bv) { std::memcpy(bytes, bv.data(), 32); }

    operator Bytes() { return {bytes, 32}; }
    operator ByteView() { return {bytes, 32}; }

    uint8_t* raw_bytes() { return bytes; }
    int length() { return 32; }

    std::string to_hex() { return silkworm::to_hex(*this); }
    static Hash from_hex(std::string hex) { return Hash(evmc::literals::internal::from_hex<bytes32>(hex.c_str())); }

    static_assert(sizeof(evmc::bytes32) == 32);
};

using Header = BlockHeader;
using BlockNum = uint64_t;
using BigInt = intx::uint256;  // use intx::to_string, from_string, ...

// using Bytes = std::basic_string<uint8_t>; already defined elsewhere
// using std::string to_hex(ByteView bytes);
// using std::optional<Bytes> from_hex(std::string_view hex) noexcept;

using time_point_t = std::chrono::time_point<std::chrono::system_clock>;
using time_dur_t = std::chrono::duration<std::chrono::system_clock>;

inline ByteView byte_view_of_string(const std::string& s) {
    return {reinterpret_cast<const uint8_t*>(s.data()), s.length()};
}

inline Bytes bytes_of_string(const std::string& s) { return Bytes(s.begin(), s.end()); }

inline std::ostream& operator<<(std::ostream& out, const silkworm::ByteView& bytes) {
    for (const auto& b : bytes) {
        out << std::hex << std::setw(2) << std::setfill('0') << int(b);
    }
    out << std::dec;
    return out;
}

inline std::ostream& operator<<(std::ostream& out, const evmc::address& addr) {
    out << silkworm::to_hex(addr);
    return out;
}

inline std::ostream& operator<<(std::ostream& out, const evmc::bytes32& b32) {
    out << silkworm::to_hex(b32);
    return out;
}

namespace rlp {
    void encode(Bytes& to, const Hash& h);
    rlp::DecodingResult decode(ByteView& from, Hash& to) noexcept;

    template <class T> void encode_vec(Bytes& to, const std::vector<T>& v);
    template <class T> size_t length_vec(const std::vector<T>& v);
    template <class T> DecodingResult decode_vec(ByteView& from, std::vector<T>& to);
}

}

#endif  // SILKWORM_TYPES_HPP
