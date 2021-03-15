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

#ifndef SILKWORM_COMMON_HASH_MAPS_HPP_
#define SILKWORM_COMMON_HASH_MAPS_HPP_

/*
Macro aliases to fast hash maps and sets, such as Abseil "Swiss tables"

The following macros are defined:
SILKWORM_FLAT_HASH_MAP
SILKWORM_FLAT_HASH_SET
SILKWORM_NODE_HASH_MAP

SILKWORM_FLAT_HASH_MAP is a hash map without pointer stability.
SILKWORM_FLAT_HASH_SET is a hash set without pointer stability.
SILKWORM_NODE_HASH_MAP is a hash map with pointer stability.

See https://abseil.io/docs/cpp/guides/container#fn:pointer-stability
*/

#if defined(__wasm__)

// Abseil is not compatible with Wasm due to its mutli-threading features,
// at least not under CMake, but see
// https://github.com/abseil/abseil-cpp/pull/721

#include <unordered_map>
#include <unordered_set>

#define SILKWORM_FLAT_HASH_MAP std::unordered_map
#define SILKWORM_FLAT_HASH_SET std::unordered_set
#define SILKWORM_NODE_HASH_MAP std::unordered_map

#else

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/node_hash_map.h>

#define SILKWORM_FLAT_HASH_MAP absl::flat_hash_map
#define SILKWORM_FLAT_HASH_SET absl::flat_hash_set
#define SILKWORM_NODE_HASH_MAP absl::node_hash_map

#endif

#endif  // SILKWORM_COMMON_HASH_MAPS_HPP_
