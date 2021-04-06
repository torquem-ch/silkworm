/*
   Copyright 2020-2021 The Silkworm Authors

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

#ifndef SILKWORM_TRIE_HASH_BUILDER_HPP_
#define SILKWORM_TRIE_HASH_BUILDER_HPP_

#include <functional>
#include <vector>

#include <silkworm/common/base.hpp>

namespace silkworm::trie {

struct NodeMask {
    uint16_t state{0};
    uint16_t tree{0};
    uint16_t hash{0};
};

struct Node {
    NodeMask mask{};
    std::vector<evmc::bytes32> hashes{};
};

// TG HashCollector2
using HashCollector = std::function<void(ByteView key_hex, const Node&)>;

// Calculates root hash of a Modified Merkle Patricia Trie.
// See Appendix D "Modified Merkle Patricia Trie" of the Yellow Paper
// and https://eth.wiki/fundamentals/patricia-tree
class HashBuilder {
  public:
    HashBuilder(const HashBuilder&) = delete;
    HashBuilder& operator=(const HashBuilder&) = delete;

    HashBuilder() = default;

    // Entries must be added in the strictly increasing lexicographic order (by key).
    // Consequently, duplicate keys are not allowed.
    // In addition, a key may not be a prefix of another key
    // (e.g. keys "ab" & "ab05" are mutually exclusive).
    void add(ByteView key, ByteView value);

    // May only be called after all entries have been added.
    // Not idempotent, may only be called once.
    evmc::bytes32 root_hash();

    HashCollector collector{nullptr};

  private:
    void gen_struct_step(ByteView curr, ByteView succ, ByteView value);

    void branch_ref(uint16_t mask);

    Bytes key_;  // unpacked – one nibble per byte
    Bytes value_;

    std::vector<NodeMask> groups_;
    std::vector<Bytes> stack_;  // node references: hashes or embedded RLPs
};

// TG DecompressNibbles
Bytes unpack_nibbles(ByteView packed);

}  // namespace silkworm::trie

#endif  // SILKWORM_TRIE_HASH_BUILDER_HPP_
