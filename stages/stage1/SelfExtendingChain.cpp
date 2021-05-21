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

#include "SelfExtendingChain.hpp"

namespace silkworm {

SelfExtendingChain::SelfExtendingChain(BlockNum highestInDb, BlockNum topSeenHeight): highestInDb_(highestInDb), topSeenHeight_(topSeenHeight) {
}

void SelfExtendingChain::highest_block_in_db(BlockNum n) {
    highestInDb_ = n;
}
BlockNum SelfExtendingChain::highest_block_in_db() {
    return highestInDb_;
}
void SelfExtendingChain::top_seen_block_height(BlockNum n) {
    topSeenHeight_ = n;
}
BlockNum SelfExtendingChain::top_seen_block_height() {
    return topSeenHeight_;
}

std::optional<GetBlockHeadersPacket> SelfExtendingChain::headers_forward() {
    // todo: implements!
    // ...
    // only for test:
    return request_skeleton();
}

// Request skeleton - Request "seed" headers
// It requests N headers starting at highestInDb with step = stride up to topSeenHeight

std::optional<GetBlockHeadersPacket> SelfExtendingChain::request_skeleton() {
    if (anchors_.size() > 16) return std::nullopt;

    if (topSeenHeight_ < highestInDb_ + stride) return std::nullopt;

    BlockNum length = (topSeenHeight_ - highestInDb_) / stride;
    if (length > max_len)
        length = max_len;

    GetBlockHeadersPacket packet;
    packet.origin = highestInDb_ + stride;
    packet.amount = length;
    packet.skip = stride;
    packet.reverse = false;

    return {packet};
}

}