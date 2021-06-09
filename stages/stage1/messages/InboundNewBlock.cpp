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

#include "InboundNewBlock.hpp"
#include "InboundNewBlockHashes.hpp"
#include "stages/stage1/rpc/SendMessageById.hpp"
#include "stages/stage1/RandomNumber.hpp"
#include "stages/stage1/stage1.hpp"
#include <silkworm/common/log.hpp>
#include <algorithm>

namespace silkworm {

InboundNewBlock::InboundNewBlock(const sentry::InboundMessage& msg): InboundMessage() {
    if (msg.id() != sentry::MessageId::NEW_BLOCK_66)
        throw std::logic_error("InboundNewBlock received wrong InboundMessage");

    reqId_ = RANDOM_NUMBER.generate_one();  // for trace purposes

    peerId_ = string_from_H512(msg.peer_id());

    ByteView data = byte_view_of_string(msg.data()); // copy for consumption
    rlp::DecodingResult err = rlp::decode(data, packet_);
    if (err != rlp::DecodingResult::kOk)
        throw rlp::rlp_error("rlp decoding error decoding NewBlock");
}

InboundMessage::reply_calls_t InboundNewBlock::execute() {
    using namespace std;
    // todo: implement!
    return {};
}

uint64_t InboundNewBlock::reqId() const {
    return reqId_;
}

std::string InboundNewBlock::content() const {
    std::stringstream content;
    content << packet_;
    return content.str();
}

}

