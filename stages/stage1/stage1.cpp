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
#include "stage1.hpp"

#include <chrono>

#include <silkworm/common/log.hpp>

#include "BlockRequestLogic.hpp"
#include "HeaderLogic.hpp"
#include "messages/InboundGetBlockHeaders.hpp"
#include "messages/OutboundGetBlockHeaders.hpp"
#include "messages/CompletionMessage.hpp"
#include "rpc/ReceiveMessages.hpp"
#include "rpc/SetStatus.hpp"
#include "ConcurrentContainers.hpp"

namespace silkworm {

Stage1::Stage1(ChainIdentity chain_identity, std::string db_path, std::string sentry_addr):
    chain_identity_(std::move(chain_identity)),
    db_{db_path},
    sentry_{sentry_addr},
    working_chain_{0,1000000} // todo: write correct start and end block (and update them!) - end=topSeenHeight, start=highestInDb
{
}

Stage1::~Stage1() {
    SILKWORM_LOG(LogLevel::Error) << "stage1 destroyed\n";
}

void Stage1::execution_loop() { // no-thread version
    using std::shared_ptr;
    using namespace std::chrono_literals;

    ConcurrentQueue<shared_ptr<Message>> messages{};

    // set chain limits
    BlockNum head_height = HeaderLogic::head_height(db_);
    working_chain_.highest_block_in_db(head_height); // the second limit will be set at the each block announcements

    // handling async rpc
    std::thread rpc_handling{[&]() {    // todo: add try...catch to trap exceptions and set exiting_=true to cause other thread exiting
      while (!exiting_) {
          auto executed_rpc = sentry_.receive_one_result();
          // check executed_rpc status?
          if (executed_rpc && executed_rpc->terminated()) {
              auto status = executed_rpc->status();
              if (status.ok())
                  SILKWORM_LOG(LogLevel::Debug) << "RPC ok, " << executed_rpc->name() << "\n";
              else
              SILKWORM_LOG(LogLevel::Warn) << "RPC failed, " << executed_rpc->name() << ", error: '" << status.error_message() << "', code: " << status.error_code()
                                    << ", details: " << status.error_details() << std::endl;
          }
      }
      SILKWORM_LOG(LogLevel::Info) << "rpc_handling thread exiting...\n";
    }};


    // set status
    auto [head_hash, head_td] = HeaderLogic::head_hash_and_total_difficulty(db_);
    auto set_status = rpc::SetStatus::make(chain_identity_.chain, chain_identity_.genesis_hash, chain_identity_.distinct_fork_numbers(), head_hash, head_td);
    set_status->on_receive_reply([&](auto& call) {
      if (!call.status().ok()) {
          exiting_ = true;
          SILKWORM_LOG(LogLevel::Critical) << "failed to set status to the remote sentry, cause:'" << call.status().error_message() << "', exiting...\n";
      }
      else
          SILKWORM_LOG(LogLevel::Debug) << "set-status reply arrived\n";
    });
    sentry_.exec_remotely(set_status);
    std::this_thread::sleep_for(3s); // wait for connection setup before submit other requests


    // start message receiving
    auto receive_messages = rpc::ReceiveMessages::make();
    receive_messages->on_receive_reply([&, receive_messages](auto& call) {
      SILKWORM_LOG(LogLevel::Debug) << "receive-messages reply arrived\n";
      if (!call.terminated()) {
          sentry::InboundMessage& reply = receive_messages->reply();
          auto message = InboundMessage::make(reply);
          if (message) {
              SILKWORM_LOG(LogLevel::Info) << "Message received from remote peer: " << *message << "\n";
              messages.push(message);
          }
      }
      else {
          exiting_ = true;
          SILKWORM_LOG(LogLevel::Critical) << "receiving messages stream interrupted, cause:'" << call.status().error_message() << "', exiting...\n";
      }
    });
    sentry_.exec_remotely(receive_messages);


    // message processing
    std::thread message_processing{[&]() {
      while (!exiting_) {
          shared_ptr<Message> message;
          bool present = messages.timed_wait_and_pop(message, 1000ms);
          if (!present) continue;   // timeout

          if (std::dynamic_pointer_cast<InboundMessage>(message)) {
              SILKWORM_LOG(LogLevel::Info) << "Processing message " << *message << "\n";
          }

          shared_ptr<SentryRpc> rpc = message->execute();
          if (!rpc) continue;

          if (std::dynamic_pointer_cast<InboundMessage>(message))
              SILKWORM_LOG(LogLevel::Info) << "Replying to incoming request " << message->name() << "\n";
          else // OutboundMessage
              SILKWORM_LOG(LogLevel::Info) << "Sending outgoing request " << *message << "\n";

          rpc->on_receive_reply([message, rpc, &messages](auto&) { // copy message and rpc to retain their lifetime (shared_ptr) [avoid rpc passing using make_shared_from_this in AsyncCall]
            SILKWORM_LOG(LogLevel::Info) << "Received rpc result of " << message->name() << "\n";
            shared_ptr<Message> completion = CompletionMessage::make(message, rpc); //message->handle_completion(call) would be dangerous... would be executed in another thread
            messages.push(completion); // coroutines would avoid this
          });

          sentry_.exec_remotely(rpc);
      }
      SILKWORM_LOG(LogLevel::Info) << "message_processing thread exiting...\n";
    }};


    // make outbound requests
    std::thread request_generation{[&]() {
      while (!exiting_) {
          shared_ptr<Message> message = std::make_shared<OutboundGetBlockHeaders>();
          messages.push(message);

          std::this_thread::sleep_for(60s);
      }
      SILKWORM_LOG(LogLevel::Info) << "request_generation thread exiting...\n";
    }};

    rpc_handling.join();
    message_processing.join();
    request_generation.join();
    SILKWORM_LOG(LogLevel::Info) << "Stage1 execution_loop exiting...\n";
}

}  // namespace silkworm

