// Generated by the gRPC C++ plugin.
// If you make any local change, they will be lost.
// source: proxy.proto

#include "proxy.pb.h"
#include "proxy.grpc.pb.h"

#include <functional>
#include <grpcpp/support/async_stream.h>
#include <grpcpp/support/async_unary_call.h>
#include <grpcpp/impl/codegen/channel_interface.h>
#include <grpcpp/impl/codegen/client_unary_call.h>
#include <grpcpp/impl/codegen/client_callback.h>
#include <grpcpp/impl/codegen/message_allocator.h>
#include <grpcpp/impl/codegen/method_handler.h>
#include <grpcpp/impl/codegen/rpc_service_method.h>
#include <grpcpp/impl/codegen/server_callback.h>
#include <grpcpp/impl/codegen/server_callback_handlers.h>
#include <grpcpp/impl/codegen/server_context.h>
#include <grpcpp/impl/codegen/service_type.h>
#include <grpcpp/impl/codegen/sync_stream.h>
namespace proxy_proto {

static const char* proxyService_method_names[] = {
  "/proxy_proto.proxyService/checkalive",
  "/proxy_proto.proxyService/encodeAndSetObject",
  "/proxy_proto.proxyService/decodeAndGetObject",
  "/proxy_proto.proxyService/deleteBlock",
  "/proxy_proto.proxyService/mainRecal",
  "/proxy_proto.proxyService/helpRecal",
  "/proxy_proto.proxyService/blockReloc",
  "/proxy_proto.proxyService/checkStep",
};

std::unique_ptr< proxyService::Stub> proxyService::NewStub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options) {
  (void)options;
  std::unique_ptr< proxyService::Stub> stub(new proxyService::Stub(channel, options));
  return stub;
}

proxyService::Stub::Stub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options)
  : channel_(channel), rpcmethod_checkalive_(proxyService_method_names[0], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_encodeAndSetObject_(proxyService_method_names[1], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_decodeAndGetObject_(proxyService_method_names[2], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_deleteBlock_(proxyService_method_names[3], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_mainRecal_(proxyService_method_names[4], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_helpRecal_(proxyService_method_names[5], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_blockReloc_(proxyService_method_names[6], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_checkStep_(proxyService_method_names[7], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  {}

::grpc::Status proxyService::Stub::checkalive(::grpc::ClientContext* context, const ::proxy_proto::CheckaliveCMD& request, ::proxy_proto::RequestResult* response) {
  return ::grpc::internal::BlockingUnaryCall< ::proxy_proto::CheckaliveCMD, ::proxy_proto::RequestResult, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_checkalive_, context, request, response);
}

void proxyService::Stub::async::checkalive(::grpc::ClientContext* context, const ::proxy_proto::CheckaliveCMD* request, ::proxy_proto::RequestResult* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::proxy_proto::CheckaliveCMD, ::proxy_proto::RequestResult, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_checkalive_, context, request, response, std::move(f));
}

void proxyService::Stub::async::checkalive(::grpc::ClientContext* context, const ::proxy_proto::CheckaliveCMD* request, ::proxy_proto::RequestResult* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_checkalive_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::proxy_proto::RequestResult>* proxyService::Stub::PrepareAsynccheckaliveRaw(::grpc::ClientContext* context, const ::proxy_proto::CheckaliveCMD& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::proxy_proto::RequestResult, ::proxy_proto::CheckaliveCMD, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_checkalive_, context, request);
}

::grpc::ClientAsyncResponseReader< ::proxy_proto::RequestResult>* proxyService::Stub::AsynccheckaliveRaw(::grpc::ClientContext* context, const ::proxy_proto::CheckaliveCMD& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsynccheckaliveRaw(context, request, cq);
  result->StartCall();
  return result;
}

::grpc::Status proxyService::Stub::encodeAndSetObject(::grpc::ClientContext* context, const ::proxy_proto::ObjectAndPlacement& request, ::proxy_proto::SetReply* response) {
  return ::grpc::internal::BlockingUnaryCall< ::proxy_proto::ObjectAndPlacement, ::proxy_proto::SetReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_encodeAndSetObject_, context, request, response);
}

void proxyService::Stub::async::encodeAndSetObject(::grpc::ClientContext* context, const ::proxy_proto::ObjectAndPlacement* request, ::proxy_proto::SetReply* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::proxy_proto::ObjectAndPlacement, ::proxy_proto::SetReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_encodeAndSetObject_, context, request, response, std::move(f));
}

void proxyService::Stub::async::encodeAndSetObject(::grpc::ClientContext* context, const ::proxy_proto::ObjectAndPlacement* request, ::proxy_proto::SetReply* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_encodeAndSetObject_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::proxy_proto::SetReply>* proxyService::Stub::PrepareAsyncencodeAndSetObjectRaw(::grpc::ClientContext* context, const ::proxy_proto::ObjectAndPlacement& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::proxy_proto::SetReply, ::proxy_proto::ObjectAndPlacement, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_encodeAndSetObject_, context, request);
}

::grpc::ClientAsyncResponseReader< ::proxy_proto::SetReply>* proxyService::Stub::AsyncencodeAndSetObjectRaw(::grpc::ClientContext* context, const ::proxy_proto::ObjectAndPlacement& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsyncencodeAndSetObjectRaw(context, request, cq);
  result->StartCall();
  return result;
}

::grpc::Status proxyService::Stub::decodeAndGetObject(::grpc::ClientContext* context, const ::proxy_proto::ObjectAndPlacement& request, ::proxy_proto::GetReply* response) {
  return ::grpc::internal::BlockingUnaryCall< ::proxy_proto::ObjectAndPlacement, ::proxy_proto::GetReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_decodeAndGetObject_, context, request, response);
}

void proxyService::Stub::async::decodeAndGetObject(::grpc::ClientContext* context, const ::proxy_proto::ObjectAndPlacement* request, ::proxy_proto::GetReply* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::proxy_proto::ObjectAndPlacement, ::proxy_proto::GetReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_decodeAndGetObject_, context, request, response, std::move(f));
}

void proxyService::Stub::async::decodeAndGetObject(::grpc::ClientContext* context, const ::proxy_proto::ObjectAndPlacement* request, ::proxy_proto::GetReply* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_decodeAndGetObject_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::proxy_proto::GetReply>* proxyService::Stub::PrepareAsyncdecodeAndGetObjectRaw(::grpc::ClientContext* context, const ::proxy_proto::ObjectAndPlacement& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::proxy_proto::GetReply, ::proxy_proto::ObjectAndPlacement, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_decodeAndGetObject_, context, request);
}

::grpc::ClientAsyncResponseReader< ::proxy_proto::GetReply>* proxyService::Stub::AsyncdecodeAndGetObjectRaw(::grpc::ClientContext* context, const ::proxy_proto::ObjectAndPlacement& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsyncdecodeAndGetObjectRaw(context, request, cq);
  result->StartCall();
  return result;
}

::grpc::Status proxyService::Stub::deleteBlock(::grpc::ClientContext* context, const ::proxy_proto::NodeAndBlock& request, ::proxy_proto::DelReply* response) {
  return ::grpc::internal::BlockingUnaryCall< ::proxy_proto::NodeAndBlock, ::proxy_proto::DelReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_deleteBlock_, context, request, response);
}

void proxyService::Stub::async::deleteBlock(::grpc::ClientContext* context, const ::proxy_proto::NodeAndBlock* request, ::proxy_proto::DelReply* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::proxy_proto::NodeAndBlock, ::proxy_proto::DelReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_deleteBlock_, context, request, response, std::move(f));
}

void proxyService::Stub::async::deleteBlock(::grpc::ClientContext* context, const ::proxy_proto::NodeAndBlock* request, ::proxy_proto::DelReply* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_deleteBlock_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::proxy_proto::DelReply>* proxyService::Stub::PrepareAsyncdeleteBlockRaw(::grpc::ClientContext* context, const ::proxy_proto::NodeAndBlock& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::proxy_proto::DelReply, ::proxy_proto::NodeAndBlock, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_deleteBlock_, context, request);
}

::grpc::ClientAsyncResponseReader< ::proxy_proto::DelReply>* proxyService::Stub::AsyncdeleteBlockRaw(::grpc::ClientContext* context, const ::proxy_proto::NodeAndBlock& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsyncdeleteBlockRaw(context, request, cq);
  result->StartCall();
  return result;
}

::grpc::Status proxyService::Stub::mainRecal(::grpc::ClientContext* context, const ::proxy_proto::mainRecalPlan& request, ::proxy_proto::RecalReply* response) {
  return ::grpc::internal::BlockingUnaryCall< ::proxy_proto::mainRecalPlan, ::proxy_proto::RecalReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_mainRecal_, context, request, response);
}

void proxyService::Stub::async::mainRecal(::grpc::ClientContext* context, const ::proxy_proto::mainRecalPlan* request, ::proxy_proto::RecalReply* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::proxy_proto::mainRecalPlan, ::proxy_proto::RecalReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_mainRecal_, context, request, response, std::move(f));
}

void proxyService::Stub::async::mainRecal(::grpc::ClientContext* context, const ::proxy_proto::mainRecalPlan* request, ::proxy_proto::RecalReply* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_mainRecal_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::proxy_proto::RecalReply>* proxyService::Stub::PrepareAsyncmainRecalRaw(::grpc::ClientContext* context, const ::proxy_proto::mainRecalPlan& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::proxy_proto::RecalReply, ::proxy_proto::mainRecalPlan, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_mainRecal_, context, request);
}

::grpc::ClientAsyncResponseReader< ::proxy_proto::RecalReply>* proxyService::Stub::AsyncmainRecalRaw(::grpc::ClientContext* context, const ::proxy_proto::mainRecalPlan& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsyncmainRecalRaw(context, request, cq);
  result->StartCall();
  return result;
}

::grpc::Status proxyService::Stub::helpRecal(::grpc::ClientContext* context, const ::proxy_proto::helpRecalPlan& request, ::proxy_proto::RecalReply* response) {
  return ::grpc::internal::BlockingUnaryCall< ::proxy_proto::helpRecalPlan, ::proxy_proto::RecalReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_helpRecal_, context, request, response);
}

void proxyService::Stub::async::helpRecal(::grpc::ClientContext* context, const ::proxy_proto::helpRecalPlan* request, ::proxy_proto::RecalReply* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::proxy_proto::helpRecalPlan, ::proxy_proto::RecalReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_helpRecal_, context, request, response, std::move(f));
}

void proxyService::Stub::async::helpRecal(::grpc::ClientContext* context, const ::proxy_proto::helpRecalPlan* request, ::proxy_proto::RecalReply* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_helpRecal_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::proxy_proto::RecalReply>* proxyService::Stub::PrepareAsynchelpRecalRaw(::grpc::ClientContext* context, const ::proxy_proto::helpRecalPlan& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::proxy_proto::RecalReply, ::proxy_proto::helpRecalPlan, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_helpRecal_, context, request);
}

::grpc::ClientAsyncResponseReader< ::proxy_proto::RecalReply>* proxyService::Stub::AsynchelpRecalRaw(::grpc::ClientContext* context, const ::proxy_proto::helpRecalPlan& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsynchelpRecalRaw(context, request, cq);
  result->StartCall();
  return result;
}

::grpc::Status proxyService::Stub::blockReloc(::grpc::ClientContext* context, const ::proxy_proto::blockRelocPlan& request, ::proxy_proto::blockRelocReply* response) {
  return ::grpc::internal::BlockingUnaryCall< ::proxy_proto::blockRelocPlan, ::proxy_proto::blockRelocReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_blockReloc_, context, request, response);
}

void proxyService::Stub::async::blockReloc(::grpc::ClientContext* context, const ::proxy_proto::blockRelocPlan* request, ::proxy_proto::blockRelocReply* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::proxy_proto::blockRelocPlan, ::proxy_proto::blockRelocReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_blockReloc_, context, request, response, std::move(f));
}

void proxyService::Stub::async::blockReloc(::grpc::ClientContext* context, const ::proxy_proto::blockRelocPlan* request, ::proxy_proto::blockRelocReply* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_blockReloc_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::proxy_proto::blockRelocReply>* proxyService::Stub::PrepareAsyncblockRelocRaw(::grpc::ClientContext* context, const ::proxy_proto::blockRelocPlan& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::proxy_proto::blockRelocReply, ::proxy_proto::blockRelocPlan, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_blockReloc_, context, request);
}

::grpc::ClientAsyncResponseReader< ::proxy_proto::blockRelocReply>* proxyService::Stub::AsyncblockRelocRaw(::grpc::ClientContext* context, const ::proxy_proto::blockRelocPlan& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsyncblockRelocRaw(context, request, cq);
  result->StartCall();
  return result;
}

::grpc::Status proxyService::Stub::checkStep(::grpc::ClientContext* context, const ::proxy_proto::AskIfSuccess& request, ::proxy_proto::RepIfSuccess* response) {
  return ::grpc::internal::BlockingUnaryCall< ::proxy_proto::AskIfSuccess, ::proxy_proto::RepIfSuccess, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_checkStep_, context, request, response);
}

void proxyService::Stub::async::checkStep(::grpc::ClientContext* context, const ::proxy_proto::AskIfSuccess* request, ::proxy_proto::RepIfSuccess* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::proxy_proto::AskIfSuccess, ::proxy_proto::RepIfSuccess, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_checkStep_, context, request, response, std::move(f));
}

void proxyService::Stub::async::checkStep(::grpc::ClientContext* context, const ::proxy_proto::AskIfSuccess* request, ::proxy_proto::RepIfSuccess* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_checkStep_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::proxy_proto::RepIfSuccess>* proxyService::Stub::PrepareAsynccheckStepRaw(::grpc::ClientContext* context, const ::proxy_proto::AskIfSuccess& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::proxy_proto::RepIfSuccess, ::proxy_proto::AskIfSuccess, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_checkStep_, context, request);
}

::grpc::ClientAsyncResponseReader< ::proxy_proto::RepIfSuccess>* proxyService::Stub::AsynccheckStepRaw(::grpc::ClientContext* context, const ::proxy_proto::AskIfSuccess& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsynccheckStepRaw(context, request, cq);
  result->StartCall();
  return result;
}

proxyService::Service::Service() {
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      proxyService_method_names[0],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< proxyService::Service, ::proxy_proto::CheckaliveCMD, ::proxy_proto::RequestResult, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](proxyService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::proxy_proto::CheckaliveCMD* req,
             ::proxy_proto::RequestResult* resp) {
               return service->checkalive(ctx, req, resp);
             }, this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      proxyService_method_names[1],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< proxyService::Service, ::proxy_proto::ObjectAndPlacement, ::proxy_proto::SetReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](proxyService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::proxy_proto::ObjectAndPlacement* req,
             ::proxy_proto::SetReply* resp) {
               return service->encodeAndSetObject(ctx, req, resp);
             }, this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      proxyService_method_names[2],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< proxyService::Service, ::proxy_proto::ObjectAndPlacement, ::proxy_proto::GetReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](proxyService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::proxy_proto::ObjectAndPlacement* req,
             ::proxy_proto::GetReply* resp) {
               return service->decodeAndGetObject(ctx, req, resp);
             }, this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      proxyService_method_names[3],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< proxyService::Service, ::proxy_proto::NodeAndBlock, ::proxy_proto::DelReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](proxyService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::proxy_proto::NodeAndBlock* req,
             ::proxy_proto::DelReply* resp) {
               return service->deleteBlock(ctx, req, resp);
             }, this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      proxyService_method_names[4],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< proxyService::Service, ::proxy_proto::mainRecalPlan, ::proxy_proto::RecalReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](proxyService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::proxy_proto::mainRecalPlan* req,
             ::proxy_proto::RecalReply* resp) {
               return service->mainRecal(ctx, req, resp);
             }, this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      proxyService_method_names[5],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< proxyService::Service, ::proxy_proto::helpRecalPlan, ::proxy_proto::RecalReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](proxyService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::proxy_proto::helpRecalPlan* req,
             ::proxy_proto::RecalReply* resp) {
               return service->helpRecal(ctx, req, resp);
             }, this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      proxyService_method_names[6],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< proxyService::Service, ::proxy_proto::blockRelocPlan, ::proxy_proto::blockRelocReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](proxyService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::proxy_proto::blockRelocPlan* req,
             ::proxy_proto::blockRelocReply* resp) {
               return service->blockReloc(ctx, req, resp);
             }, this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      proxyService_method_names[7],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< proxyService::Service, ::proxy_proto::AskIfSuccess, ::proxy_proto::RepIfSuccess, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](proxyService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::proxy_proto::AskIfSuccess* req,
             ::proxy_proto::RepIfSuccess* resp) {
               return service->checkStep(ctx, req, resp);
             }, this)));
}

proxyService::Service::~Service() {
}

::grpc::Status proxyService::Service::checkalive(::grpc::ServerContext* context, const ::proxy_proto::CheckaliveCMD* request, ::proxy_proto::RequestResult* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status proxyService::Service::encodeAndSetObject(::grpc::ServerContext* context, const ::proxy_proto::ObjectAndPlacement* request, ::proxy_proto::SetReply* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status proxyService::Service::decodeAndGetObject(::grpc::ServerContext* context, const ::proxy_proto::ObjectAndPlacement* request, ::proxy_proto::GetReply* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status proxyService::Service::deleteBlock(::grpc::ServerContext* context, const ::proxy_proto::NodeAndBlock* request, ::proxy_proto::DelReply* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status proxyService::Service::mainRecal(::grpc::ServerContext* context, const ::proxy_proto::mainRecalPlan* request, ::proxy_proto::RecalReply* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status proxyService::Service::helpRecal(::grpc::ServerContext* context, const ::proxy_proto::helpRecalPlan* request, ::proxy_proto::RecalReply* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status proxyService::Service::blockReloc(::grpc::ServerContext* context, const ::proxy_proto::blockRelocPlan* request, ::proxy_proto::blockRelocReply* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status proxyService::Service::checkStep(::grpc::ServerContext* context, const ::proxy_proto::AskIfSuccess* request, ::proxy_proto::RepIfSuccess* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}


}  // namespace proxy_proto
