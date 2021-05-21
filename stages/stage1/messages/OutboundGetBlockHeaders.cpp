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

#include "OutboundGetBlockHeaders.hpp"
#include "stages/stage1/rpc/SendMessageByMinBlock.hpp"
#include "stages/stage1/stage1.hpp"
#include <silkworm/common/log.hpp>
#include <sstream>

namespace silkworm {

OutboundGetBlockHeaders::OutboundGetBlockHeaders() {}

OutboundGetBlockHeaders::request_call_t OutboundGetBlockHeaders::execute() {
    // see TG sendHeaderRequest

    auto packet = STAGE1.working_chain().headers_forward();
    if (!packet) return nullptr;
    packet_ = *packet;

    if (std::holds_alternative<Hash>(packet_.origin))
        throw std::logic_error("OutboundGetBlockHeaders expects block number not hash");    // todo: check!

    BlockNum min_block = std::get<BlockNum>(packet_.origin);
    if (!packet_.reverse)
        min_block += packet_.amount * packet_.skip;

    auto msg_reply = std::make_unique<sentry::OutboundMessageData>();

    msg_reply->set_id(sentry::MessageId::GetBlockHeaders);

    auto rlp_encoding_len = rlp::length(packet_);
    Bytes rlp_encoding(rlp_encoding_len, 0);
    rlp::encode(rlp_encoding, packet_);
    msg_reply->set_data(rlp_encoding.data(), rlp_encoding.length()); // copy

    return rpc::SendMessageByMinBlock::make(min_block, std::move(msg_reply));
}

void OutboundGetBlockHeaders::handle_completion(SentryRpc& /*reply*/) {
    // auto& specific_reply = dynamic_cast<rpc::SendMessageById&>(reply);
    //  use specific_reply...
}

std::string OutboundGetBlockHeaders::content() const {
    std::stringstream content;
    content << packet_;
    return content.str();
}

}