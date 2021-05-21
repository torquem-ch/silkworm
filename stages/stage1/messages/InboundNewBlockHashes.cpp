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

#include "InboundNewBlockHashes.hpp"
#include "stages/stage1/stage1.hpp"
#include <algorithm>

namespace silkworm {

InboundNewBlockHashes::InboundNewBlockHashes(const sentry::InboundMessage& msg): InboundMessage() {
    if (msg.id() != sentry::MessageId::NewBlockHashes)
        throw std::logic_error("InboundNewBlockHashes received wrong InboundMessage");

    peerId_ = string_from_H512(msg.peer_id());

    ByteView data = byte_view_of_string(msg.data()); // copy for consumption
    rlp::DecodingResult err = rlp::decode(data, packet_);
    if (err != rlp::DecodingResult::kOk)
        throw rlp::rlp_error("rlp decoding error decoding NewBlockHashes");
}

InboundMessage::reply_call_t InboundNewBlockHashes::execute() {
    using namespace std;

    BlockNum max = STAGE1.working_chain().top_seen_block_height();
    for(size_t i = 0; i < packet_.size(); i++) {
        BlockNum current = packet_[i].number;
        max = std::max(max, current);
    }
    STAGE1.working_chain().top_seen_block_height(max);

    // todo: implements rest of processing if any! (see TG)

    return nullptr;
}

std::string InboundNewBlockHashes::content() const {
    std::stringstream content;
    content << packet_;
    return content.str();
}

}