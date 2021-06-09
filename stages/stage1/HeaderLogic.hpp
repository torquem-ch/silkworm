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

#ifndef SILKWORM_HEADERLOGIC_HPP
#define SILKWORM_HEADERLOGIC_HPP

#include "Types.hpp"
#include "DbTx.hpp"
#include <vector>
#include <queue>
#include <map>

namespace silkworm {

struct Link {
    std::shared_ptr<BlockHeader> header;        // Header to which this link point to
    BlockNum blockHeight;                       // Block height of the header, repeated here for convenience (remove?)
    Hash hash;                                  // Hash of the header
    std::vector<std::shared_ptr<Link>> next;    // Reverse of parentHash / Allows iteration over links in ascending block height order
    bool persisted;                             // Whether this link comes from the database record
    bool preverified;                           // Ancestor of pre-verified header
    int idx;                                    // Index in the heap (used by Go binary heap impl, remove?)
};

struct Anchor {
    Hash parentHash;                            // Hash of the header this anchor can be connected to (to disappear)
    BlockNum blockHeight;                       // block height of the anchor
    uint64_t timestamp;                         // Zero when anchor has just been created, otherwise timestamps when timeout on this anchor request expires
    int timeouts;                               // Number of timeout that this anchor has experiences - after certain threshold, it gets invalidated
    std::vector<std::shared_ptr<Link>> links;   // Links attached immediately to this anchor
};

struct Link_Older_Than: public std::binary_function<std::shared_ptr<Link>, std::shared_ptr<Link>, bool>
{
    bool operator()(const std::shared_ptr<Link>& x, const std::shared_ptr<Link>& y) const
    { return x->blockHeight < y->blockHeight; }
};

struct Link_Younger_Than: public std::binary_function<std::shared_ptr<Link>, std::shared_ptr<Link>, bool>
{
    bool operator()(const std::shared_ptr<Link>& x, const std::shared_ptr<Link>& y) const
    { return x->blockHeight > y->blockHeight; }
};

struct Anchor_Older_Than: public std::binary_function<std::shared_ptr<Anchor>, std::shared_ptr<Anchor>, bool>
{
    bool operator()(const std::shared_ptr<Anchor>& x, const std::shared_ptr<Anchor>& y) const
    { return x->timestamp < y->timestamp; }
};

using Oldest_First_Link_Queue  = std::priority_queue<std::shared_ptr<Link>,
                                                     std::vector<std::shared_ptr<Link>>,
                                                     Link_Older_Than>;

using Youngest_First_Link_Queue = std::priority_queue<std::shared_ptr<Link>,
                                                      std::vector<std::shared_ptr<Link>>,
                                                      Link_Younger_Than>;

using Oldest_First_Anchor_Queue = std::priority_queue<std::shared_ptr<Anchor>,
                                                      std::vector<std::shared_ptr<Anchor>>,
                                                      Anchor_Older_Than>;

using Link_Map = std::multimap<Hash,std::shared_ptr<Link>>;     // hash = link hash
using Anchor_Map = std::multimap<Hash,std::shared_ptr<Anchor>>; // hash = anchor *parent* hash

class HeaderLogic {     // todo: modularize this!
  public:
    static const long soft_response_limit = 2 * 1024 * 1024; // Target maximum size of returned blocks, headers or node data.
    static const long est_header_rlp_size = 500;             // Approximate size of an RLP encoded block header
    static const long max_headers_serve = 1024;              // Amount of block headers to be fetched per retrieval request

    // Headers
    static std::vector<BlockHeader> recoverByHash(Hash origin, uint64_t amount, uint64_t skip, bool reverse);
    static std::vector<BlockHeader> recoverByNumber(BlockNum origin, uint64_t amount, uint64_t skip, bool reverse);

    // Node current status
    static BlockNum                head_height(DbTx& db);
    static std::tuple<Hash,BigInt> head_hash_and_total_difficulty(DbTx& db);

    // Ancestor
    static std::tuple<Hash,BlockNum> getAncestor(DbTx& db, Hash hash, BlockNum blockNum, BlockNum ancestor, uint64_t& max_non_canonical);
};

}

#endif  // SILKWORM_HEADERLOGIC_HPP
