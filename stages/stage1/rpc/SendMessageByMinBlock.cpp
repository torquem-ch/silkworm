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

#include "SendMessageByMinBlock.hpp"

namespace silkworm::rpc {

SendMessageByMinBlock::SendMessageByMinBlock(BlockNum min_block, std::unique_ptr<sentry::OutboundMessageData> message):
    AsyncUnaryCall("SendMessageByMinBlock", &sentry::Sentry::Stub::PrepareAsyncSendMessageByMinBlock, {})
{
    request_.set_min_block(min_block);
    request_.set_allocated_data(message.release());  // take ownership
}

}