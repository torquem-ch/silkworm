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
#include "stages/stage1/rpc/SendMessageById.hpp"
#include "stages/stage1/RandomNumber.hpp"
#include "stages/stage1/stage1.hpp"
#include <silkworm/common/log.hpp>
#include <algorithm>
#include "stages/stage1/packets/RLPEth66PacketCoding.hpp"

namespace silkworm {

InboundNewBlockHashes::InboundNewBlockHashes(const sentry::InboundMessage& msg): InboundMessage() {
    if (msg.id() != sentry::MessageId::NEW_BLOCK_HASHES_66)
        throw std::logic_error("InboundNewBlockHashes received wrong InboundMessage");

    reqId_ = RANDOM_NUMBER.generate_one();  // for trace purposes

    peerId_ = string_from_H512(msg.peer_id());

    ByteView data = byte_view_of_string(msg.data()); // copy for consumption
    rlp::DecodingResult err = rlp::decode(data, packet_);
    if (err != rlp::DecodingResult::kOk)
        throw rlp::rlp_error("rlp decoding error decoding NewBlockHashes");
}

InboundMessage::reply_calls_t InboundNewBlockHashes::execute() {
    using namespace std;

    BlockNum max = STAGE1.working_chain().top_seen_block_height();

    reply_calls_t calls;

    for(size_t i = 0; i < packet_.size(); i++) {
        Hash hash = packet_[i].hash;

        // save announcement
        STAGE1.working_chain().save_external_announce(hash);
        if (STAGE1.working_chain().has_link(hash))
            continue;

        // request header
        GetBlockHeadersPacket66 reply;
        reply.requestId = RANDOM_NUMBER.generate_one();
        reply.request.origin = hash;
        reply.request.amount = 1;
        reply.request.skip = 0;
        reply.request.reverse = false;

        Bytes rlp_encoding;
        rlp::encode(rlp_encoding, reply);

        auto msg_reply = std::make_unique<sentry::OutboundMessageData>();
        msg_reply->set_id(sentry::MessageId::GET_BLOCK_HEADERS_66);
        msg_reply->set_data(rlp_encoding.data(), rlp_encoding.length()); // copy

        auto rpc = rpc::SendMessageById::make(peerId_, std::move(msg_reply));

        calls.push_back(rpc);

        // calculate top seen block height
        max = std::max(max, packet_[i].number);
    }

    STAGE1.working_chain().top_seen_block_height(max);

    return calls;
}

uint64_t InboundNewBlockHashes::reqId() const {
    return reqId_;
}

std::string InboundNewBlockHashes::content() const {
    std::stringstream content;
    content << packet_;
    return content.str();
}

}