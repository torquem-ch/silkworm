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

#include "InboundBlockHeaders.hpp"
#include "stages/stage1/rpc/PeerMinBlock.hpp"
#include "stages/stage1/HeaderLogic.hpp"
#include "stages/stage1/packets/RLPError.hpp"

namespace silkworm {


InboundBlockHeaders::InboundBlockHeaders(const sentry::InboundMessage& msg): InboundMessage() {
    if (msg.id() != sentry::MessageId::BLOCK_HEADERS_66)
        throw std::logic_error("InboundBlockHeaders received wrong InboundMessage");

    peerId_ = string_from_H512(msg.peer_id());

    ByteView data = byte_view_of_string(msg.data()); // copy for consumption
    rlp::DecodingResult err = rlp::decode(data, packet_);
    if (err != rlp::DecodingResult::kOk)
        throw rlp::rlp_error("rlp decoding error decoding BlockHeaders");
}

InboundMessage::reply_calls_t InboundBlockHeaders::execute() {
    using namespace std;

    BlockNum highestBlock = 0;
    for(Header& header: packet_.request) {
        highestBlock = std::max(highestBlock, header.number);
    }

    // todo: complete implementation!

    /* from Erigon
    if segments, penalty, err := cs.Hd.SplitIntoSegments(headersRaw, headers); err == nil {
        if penalty == headerdownload.NoPenalty {
            var canRequestMore bool
            for _, segment := range segments {
                newBlock = false;
                requestMore := cs.Hd.ProcessSegment(segment, newBlock, string(gointerfaces.ConvertH512ToBytes(in.PeerId)))
                canRequestMore = canRequestMore || requestMore
            }

            if canRequestMore {
                    currentTime := uint64(time.Now().Unix())
                    req, penalties := cs.Hd.RequestMoreHeaders(currentTime)
                    if req != nil {
                        if peer := cs.SendHeaderRequest(ctx, req); peer != nil {
                            timeOut = 5;
                            cs.Hd.SentRequest(req, currentTime, timeOut)
                            log.Debug("Sent request", "height", req.Number)
                        }
                    }
                    cs.Penalize(ctx, penalties)
                }
        } else {
            outreq := proto_sentry.PenalizePeerRequest{
                PeerId:  in.PeerId,
                Penalty: proto_sentry.PenaltyKind_Kick,
            }
            if _, err1 := sentry.PenalizePeer(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
                log.Error("Could not send penalty", "err", err1)
            }
        }
    } else {
        return fmt.Errorf("singleHeaderAsSegment failed: %v", err)
    }
    outreq := proto_sentry.PeerMinBlockRequest{
        PeerId:   in.PeerId,
        MinBlock: heighestBlock,
    }
    */

    return {std::make_shared<rpc::PeerMinBlock>(peerId_, highestBlock)};
}

void InboundBlockHeaders::handle_completion([[maybe_unused]] SentryRpc&) {
    // There is no replay
}

uint64_t InboundBlockHeaders::reqId() const {
    return packet_.requestId;
}

std::string InboundBlockHeaders::content() const {
    std::stringstream content;
    content << packet_;
    return content.str();
}


}