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

#include "InboundGetBlockBodies.hpp"
#include "stages/stage1/rpc/SendMessageById.hpp"
#include "stages/stage1/BodyLogic.hpp"
#include "stages/stage1/packets/BlockBodiesPacket.hpp"
#include "stages/stage1/packets/RLPError.hpp"
#include "stages/stage1/stage1.hpp"

namespace silkworm {


InboundGetBlockBodies::InboundGetBlockBodies(const sentry::InboundMessage& msg): InboundMessage() {
    if (msg.id() != sentry::MessageId::GET_BLOCK_BODIES_66)
        throw std::logic_error("InboundGetBlockBodies received wrong InboundMessage");

    peerId_ = string_from_H512(msg.peer_id());

    ByteView data = byte_view_of_string(msg.data()); // copy for consumption
    rlp::DecodingResult err = rlp::decode(data, packet_);
    if (err != rlp::DecodingResult::kOk)
        throw rlp::rlp_error("rlp decoding error decoding GetBlockBodies");
}


/*
 // ReplyBlockBodiesRLP is the eth/66 version of SendBlockBodiesRLP.
func (p *Peer) ReplyBlockBodiesRLP(id uint64, bodies []rlp.RawValue) error {
	// Not packed into BlockBodiesPacket to avoid RLP decoding
	return p2p.Send(p.rw, BlockBodiesMsg, BlockBodiesRLPPacket66{
		RequestId:            id,
		BlockBodiesRLPPacket: bodies,
	})
}
 */
InboundMessage::reply_calls_t InboundGetBlockBodies::execute() {
    using namespace std;

    BlockBodiesPacket66 reply;
    reply.requestId = packet_.requestId;
    reply.request = BodyLogic::recover(STAGE1.db_tx(), packet_.request);

    Bytes rlp_encoding;
    rlp::encode(rlp_encoding, reply);

    auto msg_reply = std::make_unique<sentry::OutboundMessageData>();
    msg_reply->set_id(sentry::MessageId::BLOCK_BODIES_66);
    msg_reply->set_data(rlp_encoding.data(), rlp_encoding.length()); // copy

    return {std::make_shared<rpc::SendMessageById>(peerId_, std::move(msg_reply))};

}

void InboundGetBlockBodies::handle_completion(SentryRpc& reply) {
    [[maybe_unused]] auto& specific_reply = dynamic_cast<rpc::SendMessageById&>(reply);
    // todo: use specific_reply...
}

uint64_t InboundGetBlockBodies::reqId() const {
    return packet_.requestId;
}

std::string InboundGetBlockBodies::content() const {
    std::stringstream content;
    content << packet_;
    return content.str();
}

}