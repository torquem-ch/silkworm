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

#ifndef SILKWORM_MESSAGE_HPP
#define SILKWORM_MESSAGE_HPP

#include "stages/stage1/SentryClient.hpp"
#include "stages/stage1/Types.hpp"


namespace silkworm {

class Message {
  public:
    using rpc_t = std::shared_ptr<SentryRpc>;
    using rpc_bundle_t = std::vector<rpc_t>;

    virtual std::string name() const = 0;
    virtual std::string content() const = 0;
    virtual uint64_t    reqId() const = 0;

    virtual rpc_bundle_t execute() = 0;    // inbound message does a reply, outbound message does a request

    virtual void handle_completion(SentryRpc&) {}

    virtual ~Message() = default;
};

std::ostream& operator<<(std::ostream&, const silkworm::Message&);
std::string identify(const silkworm::Message& message);

}



#endif  // SILKWORM_MESSAGE_HPP
