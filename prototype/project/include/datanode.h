#ifndef DATANODE_H
#define DATANODE_H

#include "datanode.grpc.pb.h"
#include <grpc++/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <asio.hpp>
#include <string>
#define IF_DEBUG true
// #define IF_DEBUG false
namespace ECProject
{
    class DatanodeImpl final
        : public datanode_proto::datanodeService::Service
    {
    public:
        DatanodeImpl(std::string datanode_ip_port) : datanode_ip_port(datanode_ip_port), acceptor(io_context, asio::ip::tcp::endpoint(asio::ip::address::from_string(datanode_ip_port.substr(0, datanode_ip_port.find(':')).c_str()), 20 + std::stoi(datanode_ip_port.substr(datanode_ip_port.find(':') + 1, datanode_ip_port.size()))))
        {
            m_ip = datanode_ip_port.substr(0, datanode_ip_port.find(':'));
            m_port = std::stoi(datanode_ip_port.substr(datanode_ip_port.find(':') + 1, datanode_ip_port.size()));
            m_download_port = m_port + 20;
        }
        ~DatanodeImpl(){};
        grpc::Status checkalive(
            grpc::ServerContext *context,
            const datanode_proto::CheckaliveCMD *request,
            datanode_proto::RequestResult *response) override;
        // set
        grpc::Status handleSet(
            grpc::ServerContext *context,
            const datanode_proto::SetInfo *set_info,
            datanode_proto::RequestResult *response) override;
        // get
        grpc::Status handleGet(
            grpc::ServerContext *context,
            const datanode_proto::GetInfo *get_info,
            datanode_proto::RequestResult *response) override;
        // delete
        grpc::Status handleDelete(
            grpc::ServerContext *context,
            const datanode_proto::DelInfo *del_info,
            datanode_proto::RequestResult *response) override;

    private:
        std::string datanode_ip_port;
        std::string m_ip;
        int m_port;
        int m_block_size;
        int m_download_port;
        asio::io_context io_context;
        asio::ip::tcp::acceptor acceptor;
    };

    class DataNode
    {
    public:
        DataNode(std::string datanode_ip_port) : datanode_ip_port(datanode_ip_port), m_datanodeImpl_ptr(datanode_ip_port) {}
        void Run()
        {
            grpc::EnableDefaultHealthCheckService(true);
            grpc::reflection::InitProtoReflectionServerBuilderPlugin();
            grpc::ServerBuilder builder;
            std::cout << "datanode_ip_port:" << datanode_ip_port << std::endl;
            builder.AddListeningPort(datanode_ip_port, grpc::InsecureServerCredentials());
            builder.RegisterService(&m_datanodeImpl_ptr);
            std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
            server->Wait();
        }

    private:
        std::string datanode_ip_port;
        ECProject::DatanodeImpl m_datanodeImpl_ptr;
    };
}

#endif