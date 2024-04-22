#include "datanode.h"
#include "toolbox.h"
#include <fstream>
#include <unistd.h>
#include <sys/stat.h>

namespace ECProject
{
    grpc::Status DatanodeImpl::checkalive(
        grpc::ServerContext *context,
        const datanode_proto::CheckaliveCMD *request,
        datanode_proto::RequestResult *response)
    {
        // std::cout << "[Datanode] checkalive " << request->name() << std::endl;
        response->set_message(true);
        return grpc::Status::OK;
    }

    grpc::Status DatanodeImpl::handleSet(
        grpc::ServerContext *context,
        const datanode_proto::SetInfo *set_info,
        datanode_proto::RequestResult *response)
    {
        std::string block_key = set_info->block_key();
        int block_size = set_info->block_size();
        std::string proxy_ip = set_info->proxy_ip();
        int proxy_port = set_info->proxy_port();
        auto handler = [this, proxy_ip, proxy_port](std::string block_key, int block_size) mutable
        {
            try
            {
                // char *buf = new char[block_size];
                std::vector<char> buf(block_size);
                // only send data
                asio::error_code ec;
                asio::ip::tcp::socket socket(io_context);
                acceptor.accept(socket);
                asio::read(socket, asio::buffer(buf.data(), block_size), ec);
                
                asio::error_code ignore_ec;
                socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                socket.close(ignore_ec);
                
                std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
                std::string writepath = targetdir + block_key;
                if (access(targetdir.c_str(), 0) == -1)
                {
                    mkdir(targetdir.c_str(), S_IRWXU);
                }

                std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                ofs.write(buf.data(), block_size);
                if (IF_DEBUG)
                {
                    std::cout << "[Datanode" << m_port << "][Write] successfully write " << block_key << " with " << ofs.tellp() << "bytes" << std::endl;
                }
                ofs.flush();
                ofs.close();
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };
        try
        {
            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << m_port << "][SET] ready to handle set!" << std::endl;
            }
            std::thread my_thread(handler, block_key, block_size);
            my_thread.detach();
            response->set_message(true);
        }
        catch (std::exception &e)
        {
            std::cout << "exception" << std::endl;
            std::cout << e.what() << std::endl;
        }
        return grpc::Status::OK;
    }

    grpc::Status DatanodeImpl::handleGet(
        grpc::ServerContext *context,
        const datanode_proto::GetInfo *get_info,
        datanode_proto::RequestResult *response)
    {
        std::string block_key = get_info->block_key();
        int block_size = get_info->block_size();
        std::string proxy_ip = get_info->proxy_ip();
        int proxy_port = get_info->proxy_port();
        auto handler = [this](std::string block_key, int block_size, std::string proxy_ip, int proxy_port) mutable
        {
            std::string targetdir = "./storage/" + std::to_string(m_port) + "/";
            std::string readpath = targetdir + block_key;
            if (access(readpath.c_str(), 0) == -1)
            {
                std::cout << "[Datanode" << m_port << "][Read] file does not exist!" << readpath << std::endl;
            }
            else
            {
                if (IF_DEBUG)
                {
                    std::cout << "[Datanode" << m_port << "][GET] read from the disk and write to socket with port " << proxy_port << std::endl;
                }
                char *buf = new char[block_size];
                std::ifstream ifs(readpath);
                ifs.read(buf, block_size);
                ifs.close();
                if (IF_DEBUG)
                {
                    std::cout << "[Datanode" << m_port << "][GET] read " << readpath << " with length of " << strlen(buf) << std::endl;
                }

                asio::error_code error;
                asio::ip::tcp::socket socket(io_context);
                acceptor.accept(socket);
                asio::write(socket, asio::buffer(buf, block_size), error);
                asio::error_code ignore_ec;
                socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                socket.close(ignore_ec);
                if (IF_DEBUG)
                {
                    std::cout << "[Datanode" << m_port << "][GET] write to socket!" << std::endl;
                }
                delete buf;
            }
        };
        try
        {
            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << m_port << "][GET] ready to handle get!" << std::endl;
            }
            std::thread my_thread(handler, block_key, block_size, proxy_ip, proxy_port);
            my_thread.detach();
            response->set_message(true);
        }
        catch (std::exception &e)
        {
            std::cout << "exception" << std::endl;
            std::cout << e.what() << std::endl;
        }
        return grpc::Status::OK;
    }

    grpc::Status DatanodeImpl::handleDelete(
        grpc::ServerContext *context,
        const datanode_proto::DelInfo *del_info,
        datanode_proto::RequestResult *response)
    {
        std::string block_key = del_info->block_key();
        std::string file_path = "./storage/" + std::to_string(m_port) + "/" + block_key;
        if (IF_DEBUG)
        {
            std::cout << "[Datanode" << m_port << "] File path:" << file_path << std::endl;
        }
        if (remove(file_path.c_str()))
        {
            std::cout << "[DEL] delete error!" << std::endl;
        }
        response->set_message(true);
        return grpc::Status::OK;
    }
}