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
#ifndef SILKWORM_STAGE1_HPP
#define SILKWORM_STAGE1_HPP

#include <atomic>
#include <chrono>

#include "ChainIdentity.hpp"
#include "DbTx.hpp"
#include "SentryClient.hpp"
#include "Singleton.hpp"
#include "Types.hpp"
#include "SelfExtendingChain.hpp"

namespace silkworm {

class Stage {
  public:
    virtual void execution_loop() = 0;

    void need_exit() { exiting_.store(true); }

  protected:
    std::atomic<bool> exiting_{false};
};

class Stage1 : public Stage {
    ChainIdentity chain_identity_;
    DbTx db_;
    SentryClient sentry_;
    SelfExtendingChain working_chain_;

  public:

    Stage1(ChainIdentity chain_identity, std::string db_path, std::string sentry_addr);
    Stage1(const Stage1&) = delete;
    Stage1(Stage1&&) = delete;
    ~Stage1();

    DbTx& db_tx() {return db_;}

    SentryClient& sentry() {return sentry_;}

    SelfExtendingChain& working_chain() {return working_chain_;}

    void execution_loop() override;

};

#define STAGE1 non_owning::Singleton<Stage1>::instance()

}  // namespace silkworm

#endif  // SILKWORM_STAGE1_HPP