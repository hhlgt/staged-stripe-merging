#ifndef COORDINATOR_H
#define COORDINATOR_H
#include "coordinator.grpc.pb.h"
#include "proxy.grpc.pb.h"
#include <grpc++/create_channel.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <meta_definition.h>
#include <mutex>
#include <thread>
#include <condition_variable>
// #define IF_DEBUG true
#define IF_DEBUG false
namespace ECProject
{
  class CoordinatorImpl final
      : public coordinator_proto::coordinatorService::Service
  {
  public:
    CoordinatorImpl()
    {
      m_cur_cluster_id = 0;
      m_cur_stripe_id = 0;
      m_merge_degree = 0;
    }
    ~CoordinatorImpl(){};
    grpc::Status setParameter(
        grpc::ServerContext *context,
        const coordinator_proto::Parameter *parameter,
        coordinator_proto::RepIfSetParaSuccess *setParameterReply) override;
    grpc::Status sayHelloToCoordinator(
        grpc::ServerContext *context,
        const coordinator_proto::RequestToCoordinator *helloRequestToCoordinator,
        coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator) override;
    grpc::Status checkalive(
        grpc::ServerContext *context,
        const coordinator_proto::RequestToCoordinator *helloRequestToCoordinator,
        coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator) override;
    // set
    grpc::Status uploadOriginKeyValue(
        grpc::ServerContext *context,
        const coordinator_proto::RequestProxyIPPort *keyValueSize,
        coordinator_proto::ReplyProxyIPPort *proxyIPPort) override;
    grpc::Status reportCommitAbort(
        grpc::ServerContext *context,
        const coordinator_proto::CommitAbortKey *commit_abortkey,
        coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator) override;
    grpc::Status checkCommitAbort(
        grpc::ServerContext *context,
        const coordinator_proto::AskIfSuccess *key_opp,
        coordinator_proto::RepIfSuccess *reply) override;
    // get
    grpc::Status getValue(
        grpc::ServerContext *context,
        const coordinator_proto::KeyAndClientIP *keyClient,
        coordinator_proto::RepIfGetSuccess *getReplyClient) override;
    // delete
    grpc::Status delByKey(
        grpc::ServerContext *context,
        const coordinator_proto::KeyFromClient *del_key,
        coordinator_proto::RepIfDeling *delReplyClient) override;
    grpc::Status delByStripe(
        grpc::ServerContext *context,
        const coordinator_proto::StripeIdFromClient *stripeid,
        coordinator_proto::RepIfDeling *delReplyClient) override;
    // merge
    grpc::Status requestMerge(
        grpc::ServerContext *context,
        const coordinator_proto::NumberOfStripesToMerge *numofstripe,
        coordinator_proto::RepIfMerged *mergeReplyClient) override;
    // other
    grpc::Status listStripes(
        grpc::ServerContext *context,
        const coordinator_proto::RequestToCoordinator *req,
        coordinator_proto::RepStripeIds *listReplyClient) override;

    bool init_clusterinfo(std::string m_clusterinfo_path);
    bool init_proxyinfo();
    void update_stripe_info_in_node(bool add_or_sub, int t_node_id, int stripe_id);
    int randomly_select_a_cluster(int stripe_id);
    int randomly_select_a_node(int cluster_id, int stripe_id);
    int generate_placement(int stripe_id, int block_size);
    void blocks_in_cluster(std::map<char, std::vector<ECProject::Block *>> &block_info, int cluster_id, int stripe_id);
    void find_max_group(int &max_group_id, int &max_group_num, int cluster_id, int stripe_id);
    int count_block_num(char type, int cluster_id, int stripe_id, int group_id);
    bool find_block(char type, int cluster_id, int stripe_id);

  private:
    std::mutex m_mutex;
    std::condition_variable cv;
    int m_cur_cluster_id = 0;
    int m_cur_stripe_id = 0;
    std::map<std::string, std::unique_ptr<proxy_proto::proxyService::Stub>>
        m_proxy_ptrs;
    ECSchema m_encode_parameters;
    std::unordered_map<std::string, ObjectInfo> m_object_commit_table;
    std::unordered_map<std::string, ObjectInfo> m_object_updating_table;
    std::vector<int> m_stripe_deleting_table;
    std::map<int, Cluster> m_cluster_table;
    std::map<int, Node> m_node_table;
    std::map<int, Stripe> m_stripe_table;
    int m_num_of_Clusters;
    // merge groups, for DIS and OPT, the stripes from the same group object to the selected placement scheme
    std::vector<std::vector<int>> m_merge_groups;
    std::vector<int> m_free_clusters;
    int m_merge_degree = 0;
    int m_agg_start_cid = 0;
  };

  class Coordinator
  {
  public:
    Coordinator(
        std::string m_coordinator_ip_port,
        std::string m_clusterinfo_path)
        : m_coordinator_ip_port{m_coordinator_ip_port},
          m_clusterinfo_path{m_clusterinfo_path}
    {
      m_coordinatorImpl.init_clusterinfo(m_clusterinfo_path);
      m_coordinatorImpl.init_proxyinfo();
    };
    // Coordinator
    void Run()
    {
      grpc::EnableDefaultHealthCheckService(true);
      grpc::reflection::InitProtoReflectionServerBuilderPlugin();
      grpc::ServerBuilder builder;
      std::string server_address(m_coordinator_ip_port);
      builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
      builder.RegisterService(&m_coordinatorImpl);
      std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
      std::cout << "Server listening on " << server_address << std::endl;
      server->Wait();
    }

  private:
    std::string m_coordinator_ip_port;
    std::string m_clusterinfo_path;
    ECProject::CoordinatorImpl m_coordinatorImpl;
  };
} // namespace ECProject

#endif