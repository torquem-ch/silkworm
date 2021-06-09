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

#ifndef SILKWORM_COMPLETIONMESSAGE_HPP
#define SILKWORM_COMPLETIONMESSAGE_HPP

#include "Message.hpp"

namespace silkworm {

// Special message needed to handle a message rpc completion in the same message processing thread
// coroutine would avoid avoid this

class CompletionMessage: public Message {
  public:
    CompletionMessage(std::shared_ptr<Message> waiting_completion, rpc_t completed_rpc):
        msg_waiting_completion_(waiting_completion), completed_rpc_(completed_rpc) {}

    std::string name() const override {return "CompletionMessage";}
    std::string content() const override {return "-";}
    uint64_t reqId() const override {return msg_waiting_completion_->reqId();};

    rpc_bundle_t execute() override {msg_waiting_completion_->handle_completion(*completed_rpc_); return {};}

    static std::shared_ptr<CompletionMessage> make(std::shared_ptr<Message> waiting_completion, rpc_t completed_rpc)
        {return std::make_shared<CompletionMessage>(waiting_completion, completed_rpc);}

  private:
    std::shared_ptr<Message> msg_waiting_completion_;
    rpc_t completed_rpc_;
};

}
#endif  // SILKWORM_COMPLETIONMESSAGE_HPP
