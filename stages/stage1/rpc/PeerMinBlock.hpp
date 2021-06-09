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

#ifndef SILKWORM_PEERMINBLOCK_HPP
#define SILKWORM_PEERMINBLOCK_HPP

#include "stages/stage1/SentryClient.hpp"

namespace silkworm::rpc {

class PeerMinBlock: public rpc::AsyncUnaryCall<sentry::Sentry, sentry::PeerMinBlockRequest, google::protobuf::Empty> {
  public:
    PeerMinBlock(const std::string& peerId, BlockNum minBlock);

    using SentryRpc::on_receive_reply;

    static auto make(const std::string& peerId, BlockNum minBlock)
    {return std::make_shared<PeerMinBlock>(peerId, minBlock);}
};

}

#endif  // SILKWORM_PEERMINBLOCK_HPP
