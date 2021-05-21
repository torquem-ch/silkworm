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

#ifndef SILKWORM_SETSTATUS_HPP
#define SILKWORM_SETSTATUS_HPP

#include <string>
#include <vector>
#include <silkworm/chain/config.hpp>
#include "stages/stage1/SentryClient.hpp"
#include "stages/stage1/Types.hpp"

namespace silkworm::rpc {

class SetStatus : public rpc::AsyncUnaryCall<sentry::Sentry, sentry::StatusData, google::protobuf::Empty> {
  public:
    SetStatus(ChainConfig chain, Hash genesis, std::vector<BlockNum> hard_forks, Hash best_hash, BigInt total_difficulty);

    using SentryRpc::on_receive_reply;

    static std::shared_ptr<SetStatus> make(ChainConfig chain, Hash genesis, std::vector<BlockNum> hard_forks, Hash best_hash, BigInt total_difficulty) {
        return std::make_shared<SetStatus>(chain, genesis, hard_forks, best_hash, total_difficulty);}
};

}

#endif  // SILKWORM_SETSTATUS_HPP
