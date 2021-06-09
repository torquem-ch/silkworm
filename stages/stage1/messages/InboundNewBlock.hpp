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

#ifndef SILKWORM_INBOUNDNEWBLOCK_HPP
#define SILKWORM_INBOUNDNEWBLOCK_HPP

#include "InboundMessage.hpp"
#include "stages/stage1/packets/NewBlockPacket.hpp"

namespace silkworm {

class InboundNewBlock: public InboundMessage {
  public:
    InboundNewBlock(const sentry::InboundMessage& msg);

    std::string name() const override {return "InboundNewBlock";}
    std::string content() const override;
    uint64_t reqId() const override;

    reply_calls_t execute() override;

  private:
    std::string peerId_;
    NewBlockPacket packet_;
    uint64_t reqId_;
};

}


#endif  // SILKWORM_INBOUNDNEWBLOCK_HPP
