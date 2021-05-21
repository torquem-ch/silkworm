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

#include "InboundMessage.hpp"
#include "InboundNewBlockHashes.hpp"
#include "InboundGetBlockHeaders.hpp"
#include <silkworm/common/log.hpp>
#include <iostream>

namespace silkworm {

std::shared_ptr<InboundMessage> InboundMessage::make(const sentry::InboundMessage& raw_message) {
    std::shared_ptr<InboundMessage> message;
    if (raw_message.id() == sentry::MessageId::NewBlockHashes)
        message = std::make_shared<InboundNewBlockHashes>(raw_message);
    else if (raw_message.id() == sentry::MessageId::GetBlockHeaders)
        message = std::make_shared<InboundGetBlockHeaders>(raw_message);
    else
        SILKWORM_LOG(LogLevel::Warn) << "InboundMessage " << sentry::MessageId_Name(raw_message.id()) << " received but not implemented yet\n";
    return message;
}

std::ostream& operator<<(std::ostream& os, const silkworm::InboundMessage& msg) {
    os << msg.name() << " content: " << msg.content();
    return os;
}

}



