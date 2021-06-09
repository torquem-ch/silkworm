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

#include "Message.hpp"
#include <silkworm/common/log.hpp>

namespace silkworm {

/*
Message::~Message() {
    SILKWORM_LOG(LogDebug) << "Message destroyed\n";
}
*/

std::ostream& operator<<(std::ostream& os, const silkworm::Message& msg) {
    os << msg.name() << ", content: " << msg.content();
    return os;
}

std::string identify(const silkworm::Message& message) {
    return message.name() + " reqId=" + std::to_string(message.reqId());
}

}