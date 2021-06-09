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

#include "ReceiveMessages.hpp"

namespace silkworm::rpc {

ReceiveMessages::ReceiveMessages():
    AsyncOutStreamingCall("ReceiveMessages", &sentry::Sentry::Stub::PrepareAsyncMessages, {}) {

    // previous RecvMessages
    request_.add_ids(sentry::MessageId::BLOCK_HEADERS_66);
    request_.add_ids(sentry::MessageId::BLOCK_BODIES_66);
    request_.add_ids(sentry::MessageId::NEW_BLOCK_HASHES_66);
    request_.add_ids(sentry::MessageId::NEW_BLOCK_66);
    // previous RecvUploadMessages
    request_.add_ids(sentry::MessageId::GET_BLOCK_HEADERS_66);
    request_.add_ids(sentry::MessageId::GET_BLOCK_BODIES_66);

}
}