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

#ifndef SILKWORM_GRPCASYNCCLIENT_HPP
#define SILKWORM_GRPCASYNCCLIENT_HPP

#include <iostream>
#include <chrono>
#include <string>
#include <vector>
#include <stdexcept>

#include <grpcpp/grpcpp.h>

#include <silkworm/db/access_layer.hpp>
#include <silkworm/db/stages.hpp>
#include <silkworm/db/util.hpp>
#include <silkworm/db/tables.hpp>

#include <silkworm/chain/config.hpp>

#include "Types.hpp"
#include "ConcurrentContainers.hpp"

//#define UNUSED(x) (void)(x) - Replaced by [[maybe_unused]]

namespace silkworm::rpc {

template <class STUB>
class AsyncClient;

// AsyncCall ----------------------------------------------------------------------------------------------------------
// A generic async RPC call with preparation, executing and reply handling
template <class STUB>
class AsyncCall {
  public:
    using callback_t = std::function<void(AsyncCall&)>;

    // See concrete implementation for a public constructor
    // Use AsyncClient to send this call to a remote server

    // Virtual destructor allows correct child destruction
    virtual ~AsyncCall() = default;

    void on_receive_reply(callback_t f) {callback_ = f;}

    virtual bool terminated() { return terminated_; };

    grpc::Status status() {
        if (!terminated_)
            throw std::logic_error("AsyncCall status not ready");
        return status_;
    }

    void* tag() {return tag_;}

    std::string name() {return name_;}

  protected:
    friend class AsyncClient<STUB>; // only to give access to start & reply_received

    // Start the remote proc call sending request to the server
    virtual void start(typename STUB::Stub* stub, grpc::CompletionQueue* cq) = 0;

    // Will be called on response arrival from the server
    virtual void reply_received(bool ok) = 0;

    // See concrete implementations for a public constructor
    AsyncCall(std::string name): name_(std::move(name)) { tag_ = static_cast<void*>(this); }

    static AsyncCall<STUB>* detag(void* tag) {return static_cast<AsyncCall<STUB>*>(tag);}  // UNSAFE

    // Name, used for logging purposes
    std::string name_;

    // Tag used by GRPCAsyncClient to identify concrete instances in the receiving loop
    void* tag_;

    // Call-context, it could be used to convey extra information to the server and/or tweak certain RPC behaviors
    grpc::ClientContext context_;

    // Status of the RPC upon completion
    grpc::Status status_;

    // Callback that will be called on completion (i.e. response arrival)
    callback_t callback_;

    bool terminated_ = false;

    std::shared_ptr<AsyncCall<STUB>> pin_;
};

// AsyncClient ----------------------------------------------------------------------------------------------------
// A generic async RPC client that is able to execute and complete an async call
template <class STUB>
class AsyncClient {
  public:
    using stub_t = STUB;
    using call_t = AsyncCall<STUB>;

    explicit AsyncClient(std::shared_ptr<grpc::Channel> channel)
        : channel_(channel), stub_(stub_t::NewStub(channel)) {}

    // send an remote call asynchronously
    void exec_remotely(std::shared_ptr<call_t> call) {
        call->pin_ = call; // trick to save shared_ptr and extend call lifetime without using a queue, not so beautiful, todo: improve
        call->start(stub_.get(), &completionQueue_);
    }

    // wait for a response and complete it; put this call in a infinite loop:
    //    while(client.receive_one_result())
    //       ; // does nothing
    std::shared_ptr<call_t> receive_one_result() {
        void* got_tag;
        bool ok = false;

        // Block until the next result is available in the completion queue
        bool got_event = completionQueue_.Next(&got_tag, &ok);  // todo: use AsyncNext to support a timeout
        if (!got_event) // the queue is fully drained and shut down
            return nullptr;

        // The tag in this example is the memory location of the call object
        call_t* raw_call = call_t::detag(got_tag); // UNSAFE
        std::shared_ptr<call_t> call = raw_call->pin_;

        // Verify that the request was completed successfully. Note that "ok"
        // corresponds solely to the request for updates introduced by Finish().
        // GPR_ASSERT(ok);  // works only for unary call

        // Delegate status & reply handling to the call
        call->reply_received(ok);

        if (call->terminated())
            call->pin_.reset();

        // Return the call for custom processing or deletion
        return call;
    }

  protected:
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<typename STUB::Stub> stub_; // the stub class generated from grpc proto
    grpc::CompletionQueue completionQueue_;  // receives async-call completion events from the gRPC runtime
};

// AsyncUnaryCall -----------------------------------------------------------------------------------------------------
template <class STUB, class REQUEST, class REPLY>
class AsyncUnaryCall: public AsyncCall<STUB> {
  public:
    using call_t = AsyncCall<STUB>;
    using request_t = REQUEST;
    using reply_t = REPLY;
    using response_reader_t = std::unique_ptr<grpc::ClientAsyncResponseReader<reply_t>>;
    using prepare_call_t = response_reader_t (STUB::Stub::*)(grpc::ClientContext* context, const request_t& request, grpc::CompletionQueue* cq);
    //using callback_t = std::function<void(AsyncUnaryCall&)>;
    using call_t::context_, call_t::status_, call_t::tag_;

    AsyncUnaryCall(std::string name, prepare_call_t pc, request_t request) : call_t{std::move(name)}, prepare_call_{pc}, request_{std::move(request)} {
    }

    virtual ~AsyncUnaryCall() = default;

    //void on_complete(callback_t f) {callback_ = f;}

    reply_t& reply() {return reply_;}

  protected:

    void start(typename STUB::Stub* stub, grpc::CompletionQueue* cq) override {
        response_reader_ = (stub->*prepare_call_)(&context_, request_, cq);  // creates an RPC object

        response_reader_->StartCall();  // initiates the RPC call

        response_reader_->Finish(&reply_, &status_, tag_);  // communicate replay & status slots and tag
    }

    void reply_received([[maybe_unused]] bool ok) override {
        call_t::terminated_ = true;

        // use status & reply
        if (call_t::callback_)
            call_t::callback_(*this);
    }

    prepare_call_t prepare_call_; // Pointer to the prepare call method of the Stub

    response_reader_t response_reader_; // return type of the prepare call method of the Stub

    request_t request_; // Container for the request we send to the server

    reply_t reply_; // Container for the data we expect from the server.

    //callback_t callback_; // Callback that will be called on completion (i.e. response arrival)
};

// AsyncOutStreamingCall ----------------------------------------------------------------------------------------------
// A generic async RPC call with preparation, executing and reply handling
template <class STUB, class REQUEST, class REPLY>
class AsyncOutStreamingCall: public AsyncCall<STUB> {
  public:
    using call_t = AsyncCall<STUB>;
    using request_t = REQUEST;
    using reply_t = REPLY;
    using response_reader_t = std::unique_ptr<::grpc::ClientAsyncReader<reply_t>>;
    using prepare_call_t = response_reader_t (STUB::Stub::*)(grpc::ClientContext* context, const request_t& request, grpc::CompletionQueue* cq);
    //using callback_t = std::function<void(AsyncOutStreamingCall&)>;
    using call_t::context_, call_t::status_, call_t::tag_;

    AsyncOutStreamingCall(std::string name, prepare_call_t pc, const request_t& request): call_t{std::move(name)}, prepare_call_{pc}, request_{std::move(request)} {
    }

    ~AsyncOutStreamingCall() {
        if (!call_t::terminated_) {
            response_reader_->Finish(&status_, tag_);  // communicate replay & status slots and tag
            call_t::terminated_ = true;
        }
        // no one will be able to check status_.ok(), maybe we need to have a stop method / todo: provide a stop method
    }

    //void on_complete(callback_t f) {callback_ = f;}

    reply_t& reply() {return reply_;}

  protected:
    void start(typename STUB::Stub* stub, grpc::CompletionQueue* cq) override {
        response_reader_ = (stub->*prepare_call_)(&context_, request_, cq);  // creates an RPC object

        response_reader_->StartCall(tag_);  // initiates the RPC call
    }

    void reply_received(bool ok) override {
        if (!ok) {         // todo: check if it is correct!
            response_reader_->Finish(&status_, tag_);   // this will populate status with the error condition
            call_t::terminated_ = true;
        }

        if (!started_) {
            started_ = true;
            response_reader_->Read(&reply_, tag_); // we have an output stream so we need call Read
            return;
        }

        // use status & reply
        if (call_t::callback_)
            call_t::callback_(*this);

        if (call_t::terminated_)
            return;     // exceptional path

        // todo: erase reply_ ?
        reply_ = {};

        // request next message
        response_reader_->Read(&reply_, tag_); // we have an output stream so we need call Read
    }

    prepare_call_t prepare_call_; // Pointer to the prepare call method of the Stub

    response_reader_t response_reader_; // return type of the prepare call method of the Stub

    request_t request_; // Container for the request we send to the server

    reply_t reply_; // Container for the data we expect from the server

    bool started_ = false;
    //callback_t callback_; // Callback that will be called on completion (i.e. response arrival)
};

} // namespace end

#endif  // SILKWORM_GRPCASYNCCLIENT_HPP
