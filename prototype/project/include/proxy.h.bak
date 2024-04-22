#ifndef PROXY_H
#define PROXY_H
#include "coordinator.grpc.pb.h"
#include "proxy.grpc.pb.h"
#include "datanode.grpc.pb.h"
#include "devcommon.h"
#include "meta_definition.h"
#include "lrc.h"
#include <asio.hpp>
#include <grpc++/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <thread>
#include <semaphore.h>
#define IF_DEBUG true
// #define IF_DEBUG false
namespace ECProject
{
  class ProxyImpl final
      : public proxy_proto::proxyService::Service,
        public std::enable_shared_from_this<ECProject::ProxyImpl>
  {

  public:
    ProxyImpl(std::string proxy_ip_port, std::string config_path, std::string coordinator_address) : config_path(config_path), proxy_ip_port(proxy_ip_port), acceptor(io_context, asio::ip::tcp::endpoint(asio::ip::address::from_string(proxy_ip_port.substr(0, proxy_ip_port.find(':')).c_str()), 1 + std::stoi(proxy_ip_port.substr(proxy_ip_port.find(':') + 1, proxy_ip_port.size())))), m_coordinator_address(coordinator_address)
    {
      init_coordinator();
      init_datanodes(config_path);
      m_merge_step_processing[0] = false;
      m_merge_step_processing[1] = false;
      m_merge_step_processing[2] = false;
      m_ip = proxy_ip_port.substr(0, proxy_ip_port.find(':'));
      m_port = std::stoi(proxy_ip_port.substr(proxy_ip_port.find(':') + 1, proxy_ip_port.size()));
      std::cout << "Cluster id:" << m_self_cluster_id << std::endl;
    }
    ~ProxyImpl(){};
    grpc::Status checkalive(
        grpc::ServerContext *context,
        const proxy_proto::CheckaliveCMD *request,
        proxy_proto::RequestResult *response) override;
    // encode and set
    grpc::Status encodeAndSetObject(
        grpc::ServerContext *context,
        const proxy_proto::ObjectAndPlacement *object_and_placement,
        proxy_proto::SetReply *response) override;
    // decode and get
    grpc::Status decodeAndGetObject(
        grpc::ServerContext *context,
        const proxy_proto::ObjectAndPlacement *object_and_placement,
        proxy_proto::GetReply *response) override;
    // delete
    grpc::Status deleteBlock(
        grpc::ServerContext *context,
        const proxy_proto::NodeAndBlock *node_and_block,
        proxy_proto::DelReply *response) override;
    // lrcwidestripe, merge
    // parity block recalculation
    grpc::Status mainRecal(
        grpc::ServerContext *context,
        const proxy_proto::mainRecalPlan *main_recal_plan,
        proxy_proto::RecalReply *response) override;
    grpc::Status helpRecal(
        grpc::ServerContext *context,
        const proxy_proto::helpRecalPlan *help_recal_plan,
        proxy_proto::RecalReply *response) override;
    // block relocation
    grpc::Status blockReloc(
        grpc::ServerContext *context,
        const proxy_proto::blockRelocPlan *block_reloc_plan,
        proxy_proto::blockRelocReply *response) override;
    // check
    grpc::Status checkStep(
        grpc::ServerContext *context,
        const proxy_proto::AskIfSuccess *step,
        proxy_proto::RepIfSuccess *response) override;
    bool SetToDatanode(const char *key, size_t key_length, const char *value, size_t value_length, const char *ip, int port, int offset);
    bool GetFromDatanode(const char *key, size_t key_length, char *value, size_t value_length, const char *ip, int port, int offset);
    bool DelInDatanode(std::string key, std::string node_ip_port);

  private:
    std::mutex m_mutex;
    std::condition_variable cv;
    bool m_merge_step_processing[3];
    bool init_coordinator();
    bool init_datanodes(std::string datanodeinfo_path);
    std::unique_ptr<coordinator_proto::coordinatorService::Stub> m_coordinator_ptr;
    std::map<std::string, std::unique_ptr<datanode_proto::datanodeService::Stub>> m_datanode_ptrs;
    std::string config_path;
    std::string proxy_ip_port;
    std::string m_ip;
    int m_port;
    int m_self_cluster_id;
    asio::io_context io_context;
    asio::ip::tcp::acceptor acceptor;
    sem_t sem;
    std::string m_coordinator_address;
  };

  class Proxy
  {
  public:
    Proxy(std::string proxy_ip_port, std::string config_path, std::string coordinator_address) : proxy_ip_port(proxy_ip_port), m_proxyImpl_ptr(proxy_ip_port, config_path, coordinator_address) {}
    void Run()
    {
      grpc::EnableDefaultHealthCheckService(true);
      grpc::reflection::InitProtoReflectionServerBuilderPlugin();
      grpc::ServerBuilder builder;
      std::cout << "proxy_ip_port:" << proxy_ip_port << std::endl;
      builder.AddListeningPort(proxy_ip_port, grpc::InsecureServerCredentials());
      builder.RegisterService(&m_proxyImpl_ptr);
      std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
      server->Wait();
    }

  private:
    std::string proxy_ip_port;
    ECProject::ProxyImpl m_proxyImpl_ptr;
  };
} // namespace ECProject
#endif