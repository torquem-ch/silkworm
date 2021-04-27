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

#ifndef SILKWORM_COMMON_CAST_HPP_
#define SILKWORM_COMMON_CAST_HPP_

// Utilities for type casting

#include <cstring>
#include <type_traits>

#include <silkworm/common/base.hpp>

namespace silkworm {

// Backport of C++20 std::bit_cast
// https://en.cppreference.com/w/cpp/numeric/bit_cast
template <class To, class From>
To bit_cast(const From& src) noexcept {
    static_assert(sizeof(To) == sizeof(From), "bit_cast requires source and destination to be the same size");
    static_assert(std::is_trivially_copyable_v<From>, "bit_cast requires the source type to be copyable");
    static_assert(std::is_trivially_copyable_v<To>, "bit_cast requires the destination type to be copyable");

    To dst;
    std::memcpy(&dst, &src, sizeof(To));
    return dst;
}

}  // namespace silkworm

#endif  // SILKWORM_COMMON_CAST_HPP_
