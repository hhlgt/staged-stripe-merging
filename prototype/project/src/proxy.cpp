#include "proxy.h"
#include "jerasure.h"
#include "reed_sol.h"
#include "tinyxml2.h"
#include "toolbox.h"
#include "lrc.h"
#include <thread>
#include <cassert>
#include <string>
#include <fstream>
template <typename T>
inline T ceil(T const &A, T const &B)
{
  return T((A + B - 1) / B);
};
namespace ECProject
{
  bool ProxyImpl::init_coordinator()
  {
    m_coordinator_ptr = coordinator_proto::coordinatorService::NewStub(grpc::CreateChannel(m_coordinator_address, grpc::InsecureChannelCredentials()));
    // coordinator_proto::RequestToCoordinator req;
    // coordinator_proto::ReplyFromCoordinator rep;
    // grpc::ClientContext context;
    // std::string proxy_info = "Proxy [" + proxy_ip_port + "]";
    // req.set_name(proxy_info);
    // grpc::Status status;
    // status = m_coordinator_ptr->checkalive(&context, req, &rep);
    // if (status.ok())
    // {
    //   std::cout << "[Coordinator Check] ok from " << m_coordinator_address << std::endl;
    // }
    // else
    // {
    //   std::cout << "[Coordinator Check] failed to connect " << m_coordinator_address << std::endl;
    // }
    return true;
  }

  bool ProxyImpl::init_datanodes(std::string m_datanodeinfo_path)
  {
    tinyxml2::XMLDocument xml;
    xml.LoadFile(m_datanodeinfo_path.c_str());
    tinyxml2::XMLElement *root = xml.RootElement();
    for (tinyxml2::XMLElement *cluster = root->FirstChildElement(); cluster != nullptr; cluster = cluster->NextSiblingElement())
    {
      std::string cluster_id(cluster->Attribute("id"));
      std::string proxy(cluster->Attribute("proxy"));
      if (proxy == proxy_ip_port)
      {
        m_self_cluster_id = std::stoi(cluster_id);
      }
      for (tinyxml2::XMLElement *node = cluster->FirstChildElement()->FirstChildElement(); node != nullptr; node = node->NextSiblingElement())
      {
        std::string node_uri(node->Attribute("uri"));
        auto _stub = datanode_proto::datanodeService::NewStub(grpc::CreateChannel(node_uri, grpc::InsecureChannelCredentials()));
        // datanode_proto::CheckaliveCMD cmd;
        // datanode_proto::RequestResult result;
        // grpc::ClientContext context;
        // std::string proxy_info = "Proxy [" + proxy_ip_port + "]";
        // cmd.set_name(proxy_info);
        // grpc::Status status;
        // status = _stub->checkalive(&context, cmd, &result);
        // if (status.ok())
        // {
        //   // std::cout << "[Datanode Check] ok from " << node_uri << std::endl;
        // }
        // else
        // {
        //   std::cout << "[Datanode Check] failed to connect " << node_uri << std::endl;
        // }
        m_datanode_ptrs.insert(std::make_pair(node_uri, std::move(_stub)));
      }
    }
    return true;
  }

  grpc::Status ProxyImpl::checkalive(grpc::ServerContext *context,
                                     const proxy_proto::CheckaliveCMD *request,
                                     proxy_proto::RequestResult *response)
  {

    std::cout << "[Proxy] checkalive" << request->name() << std::endl;
    response->set_message(false);
    init_coordinator();
    return grpc::Status::OK;
  }

  bool ProxyImpl::SetToDatanode(const char *key, size_t key_length, const char *value, size_t value_length, const char *ip, int port, int offset)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::SetInfo set_info;
      datanode_proto::RequestResult result;
      set_info.set_block_key(std::string(key));
      set_info.set_block_size(value_length);
      set_info.set_proxy_ip(m_ip);
      set_info.set_proxy_port(m_port + offset);
      set_info.set_ispull(false);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleSet(&context, set_info, &result);

      asio::error_code error;
      asio::io_context io_context;
      asio::ip::tcp::socket socket(io_context);
      asio::ip::tcp::resolver resolver(io_context);
      asio::error_code con_error;
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + 20)}), con_error);
      if (!con_error && IF_DEBUG)
      {
        std::cout << "Connect to " << ip << ":" << port + 20 << " success!" << std::endl;
      }

      asio::write(socket, asio::buffer(value, value_length), error);

      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                  << "Write " << key << " to socket finish! With length of " << strlen(value) << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  bool ProxyImpl::GetFromDatanode(const char *key, size_t key_length, char *value, size_t value_length, const char *ip, int port, int offset)
  {
    try
    {
      // ready to recieve
      char *buf = new char[value_length];
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Ready to recieve data from datanode " << std::endl;
      }

      grpc::ClientContext context;
      datanode_proto::GetInfo get_info;
      datanode_proto::RequestResult result;
      get_info.set_block_key(std::string(key));
      get_info.set_block_size(value_length);
      get_info.set_proxy_ip(m_ip);
      get_info.set_proxy_port(m_port + offset);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleGet(&context, get_info, &result);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Call datanode to handle get " << key << std::endl;
      }

      asio::io_context io_context;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::socket socket(io_context);
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + 20)}));
      asio::error_code ec;
      asio::read(socket, asio::buffer(buf, value_length), ec);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Read data from socket with length of " << value_length << std::endl;
      }
      memcpy(value, buf, value_length);
      delete buf;
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  bool ProxyImpl::DelInDatanode(std::string key, std::string node_ip_port)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::DelInfo delinfo;
      datanode_proto::RequestResult response;
      delinfo.set_block_key(key);
      grpc::Status status = m_datanode_ptrs[node_ip_port]->handleDelete(&context, delinfo, &response);
      if (status.ok() && IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][DEL] delete block " << key << " success!" << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  bool ProxyImpl::BlockRelocation(const char *key, size_t value_length, const char *src_ip, int src_port, const char *des_ip, int des_port)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::GetInfo get_info;
      datanode_proto::RequestResult result;
      get_info.set_block_key(std::string(key));
      get_info.set_block_size(value_length);
      get_info.set_proxy_ip(m_ip);
      get_info.set_proxy_port(m_port);
      std::string s_node_ip_port = std::string(src_ip) + ":" + std::to_string(src_port);
      grpc::Status stat = m_datanode_ptrs[s_node_ip_port]->handleGet(&context, get_info, &result);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Relocation]"
                  << " Call datanode" << src_port << " to handle get " << key << std::endl;
      }

      grpc::ClientContext s_context;
      datanode_proto::SetInfo set_info;
      datanode_proto::RequestResult s_result;
      set_info.set_block_key(std::string(key));
      set_info.set_block_size(value_length);
      set_info.set_proxy_ip(src_ip);
      set_info.set_proxy_port(src_port + 20);
      set_info.set_ispull(true);
      std::string d_node_ip_port = std::string(des_ip) + ":" + std::to_string(des_port);
      grpc::Status s_stat = m_datanode_ptrs[d_node_ip_port]->handleSet(&s_context, set_info, &s_result);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Relocation]"
                  << " Call datanode" << des_port << " to handle set " << key << std::endl;
      }
      if (s_stat.ok() && IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Relocation] relocate block " << key << " success!" << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }
    return true;
  }

  grpc::Status ProxyImpl::encodeAndSetObject(
      grpc::ServerContext *context,
      const proxy_proto::ObjectAndPlacement *object_and_placement,
      proxy_proto::SetReply *response)
  {
    std::string key = object_and_placement->key();
    int value_size_bytes = object_and_placement->valuesizebyte();
    int k = object_and_placement->k();
    int g_m = object_and_placement->g_m();
    int l = object_and_placement->l();
    // int stripe_id = object_and_placement->stripe_id();
    int block_size = object_and_placement->block_size();
    ECProject::EncodeType encode_type = (ECProject::EncodeType)object_and_placement->encode_type();
    std::vector<std::pair<std::string, std::pair<std::string, int>>> keys_nodes;
    for (int i = 0; i < object_and_placement->datanodeip_size(); i++)
    {
      keys_nodes.push_back(std::make_pair(object_and_placement->blockkeys(i), std::make_pair(object_and_placement->datanodeip(i), object_and_placement->datanodeport(i))));
    }
    auto encode_and_save = [this, key, value_size_bytes, k, g_m, l, block_size, keys_nodes, encode_type]() mutable
    {
      try
      {
        // read the key and value in the socket sent by client
        asio::ip::tcp::socket socket_data(io_context);
        acceptor.accept(socket_data);
        asio::error_code error;

        int extend_value_size_byte = block_size * k;
        std::vector<char> buf_key(key.size());
        std::vector<char> v_buf(extend_value_size_byte);
        for (int i = value_size_bytes; i < extend_value_size_byte; i++)
        {
          v_buf[i] = '0';
        }

        asio::read(socket_data, asio::buffer(buf_key, key.size()), error);
        if (error == asio::error::eof)
        {
          std::cout << "error == asio::error::eof" << std::endl;
        }
        else if (error)
        {
          throw asio::system_error(error);
        }
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << "Check key " << buf_key.data() << std::endl;
        }
        // check the key
        bool flag = true;
        for (int i = 0; i < int(key.size()); i++)
        {
          if (key[i] != buf_key[i])
          {
            flag = false;
          }
        }
        if (flag)
        {
          if (IF_DEBUG)
          {
            std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                      << "Read value of " << buf_key.data() << std::endl;
          }
          asio::read(socket_data, asio::buffer(v_buf.data(), value_size_bytes), error);
        }
        asio::error_code ignore_ec;
        socket_data.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
        socket_data.close(ignore_ec);

        // set the blocks to the datanode
        char *buf = v_buf.data();
        auto send_to_datanode = [this](int j, int k, std::string block_key, char **data, char **coding, int block_size, std::pair<std::string, int> ip_and_port)
        {
          if (IF_DEBUG)
          {
            std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                      << "Thread " << j << " send " << block_key << " to Datanode" << ip_and_port.second << std::endl;
          }
          if (j < k)
          {
            SetToDatanode(block_key.c_str(), block_key.size(), data[j], block_size, ip_and_port.first.c_str(), ip_and_port.second, j + 2);
          }
          else
          {
            SetToDatanode(block_key.c_str(), block_key.size(), coding[j - k], block_size, ip_and_port.first.c_str(), ip_and_port.second, j + 2);
          }
        };
        // calculate parity blocks
        std::vector<char *> v_data(k);
        std::vector<char *> v_coding(g_m + l + 1);
        char **data = (char **)v_data.data();
        char **coding = (char **)v_coding.data();

        std::vector<std::vector<char>> v_coding_area(g_m + l + 1, std::vector<char>(block_size));
        for (int j = 0; j < k; j++)
        {
          data[j] = &buf[j * block_size];
        }
        for (int j = 0; j < g_m + l + 1; j++)
        {
          coding[j] = v_coding_area[j].data();
        }
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << "Encode value with size of " << v_buf.size() << std::endl;
        }
        int send_num;
        if (encode_type == Azure_LRC || encode_type == Optimal_Cauchy_LRC)
        {
          encode(k, g_m, l, data, coding, block_size, encode_type);
          send_num = k + g_m + l;
        }
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << "Distribute blocks to datanodes" << std::endl;
        }
        std::vector<std::thread> senders;
        for (int j = 0; j < send_num; j++)
        {
          std::string block_key = keys_nodes[j].first;
          std::pair<std::string, int> &ip_and_port = keys_nodes[j].second;
          senders.push_back(std::thread(send_to_datanode, j, k, block_key, data, coding, block_size, ip_and_port));
        }
        for (int j = 0; j < int(senders.size()); j++)
        {
          senders[j].join();
        }
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << "Finish distributing blocks!" << std::endl;
        }
        coordinator_proto::CommitAbortKey commit_abort_key;
        coordinator_proto::ReplyFromCoordinator result;
        grpc::ClientContext context;
        ECProject::OpperateType opp = SET;
        commit_abort_key.set_opp(opp);
        commit_abort_key.set_key(key);
        commit_abort_key.set_ifcommitmetadata(true);
        grpc::Status status;
        status = m_coordinator_ptr->reportCommitAbort(&context, commit_abort_key, &result);
        if (status.ok() && IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << "[SET] report to coordinator success" << std::endl;
        }
        else
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << " report to coordinator fail!" << std::endl;
        }
      }
      catch (std::exception &e)
      {
        std::cout << "exception in encode_and_save" << std::endl;
        std::cout << e.what() << std::endl;
      }
    };
    try
    {
      if (IF_DEBUG)
      {
        std::cout << "[Proxy][SET] Handle encode and set" << std::endl;
      }
      std::thread my_thread(encode_and_save);
      my_thread.detach();
    }
    catch (std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cout << e.what() << std::endl;
    }

    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::decodeAndGetObject(
      grpc::ServerContext *context,
      const proxy_proto::ObjectAndPlacement *object_and_placement,
      proxy_proto::GetReply *response)
  {
    ECProject::EncodeType encode_type = (ECProject::EncodeType)object_and_placement->encode_type();
    std::string key = object_and_placement->key();
    int k = object_and_placement->k();
    int g_m = object_and_placement->g_m();
    int l = object_and_placement->l();
    // int block_size = object_and_placement->block_size();
    int value_size_bytes = object_and_placement->valuesizebyte();
    int block_size = ceil(value_size_bytes, k);
    std::string clientip = object_and_placement->clientip();
    int clientport = object_and_placement->clientport();
    int stripe_id = object_and_placement->stripe_id();

    std::vector<std::pair<std::string, std::pair<std::string, int>>> keys_nodes;
    std::vector<int> block_idxs;
    for (int i = 0; i < object_and_placement->datanodeip_size(); i++)
    {
      block_idxs.push_back(object_and_placement->blockids(i));
      keys_nodes.push_back(std::make_pair(object_and_placement->blockkeys(i), std::make_pair(object_and_placement->datanodeip(i), object_and_placement->datanodeport(i))));
    }

    auto decode_and_get = [this, key, k, g_m, l, block_size, value_size_bytes, stripe_id,
                           clientip, clientport, keys_nodes, block_idxs, encode_type]() mutable
    {
      int expect_block_number = (encode_type == Azure_LRC) ? (k + l) : k;
      int all_expect_blocks = (encode_type == Azure_LRC) ? (k + g_m + l) : (k + g_m);

      auto blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
      auto blocks_key_ptr = std::make_shared<std::vector<std::string>>();
      auto blocks_idx_ptr = std::make_shared<std::vector<int>>();
      auto myLock_ptr = std::make_shared<std::mutex>();
      auto cv_ptr = std::make_shared<std::condition_variable>();

      std::vector<char *> v_data(k);
      std::vector<char *> v_coding(all_expect_blocks - k);
      char **data = v_data.data();
      char **coding = v_coding.data();

      auto getFromNode = [this, k, blocks_ptr, blocks_key_ptr, blocks_idx_ptr, myLock_ptr, cv_ptr](int expect_block_number, int block_idx, std::string block_key, int block_size, std::string ip, int port)
      {
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                    << "Block " << block_idx << " with key " << block_key << " from Datanode" << ip << ":" << port << std::endl;
        }

        std::vector<char> temp(block_size);
        bool ret = GetFromDatanode(block_key.c_str(), block_key.size(), temp.data(), block_size, ip.c_str(), port, block_idx + 2);

        if (!ret)
        {
          std::cout << "getFromNode !ret" << std::endl;
          return;
        }
        myLock_ptr->lock();
        // get any k blocks and decode
        if (!check_received_block(k, expect_block_number, blocks_idx_ptr, blocks_ptr->size()))
        {
          blocks_ptr->push_back(temp);
          blocks_key_ptr->push_back(block_key);
          blocks_idx_ptr->push_back(block_idx);
          if (check_received_block(k, expect_block_number, blocks_idx_ptr, blocks_ptr->size()))
          {
            cv_ptr->notify_all();
          }
        }
        // get all the blocks
        // blocks_ptr->push_back(temp);
        // blocks_key_ptr->push_back(block_key);
        // blocks_idx_ptr->push_back(block_idx);
        myLock_ptr->unlock();
      };

      std::vector<std::vector<char>> v_data_area(k, std::vector<char>(block_size));
      std::vector<std::vector<char>> v_coding_area(all_expect_blocks - k, std::vector<char>(block_size));
      for (int j = 0; j < k; j++)
      {
        data[j] = v_data_area[j].data();
      }
      for (int j = 0; j < all_expect_blocks - k; j++)
      {
        coding[j] = v_coding_area[j].data();
      }
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "ready to get blocks from datanodes!" << std::endl;
      }
      std::vector<std::thread> read_treads;
      // for (int j = 0; j < k; j++)
      for (int j = 0; j < all_expect_blocks; j++)
      {
        int block_idx = block_idxs[j];
        std::string block_key = keys_nodes[j].first;
        std::pair<std::string, int> &ip_and_port = keys_nodes[j].second;
        // std::vector<char> temp(block_size);
        // GetFromDatanode(block_key.c_str(), block_key.size(), temp.data(), block_size, ip_and_port.first.c_str(), ip_and_port.second, j + 2);
        // blocks_ptr->push_back(temp);
        // blocks_key_ptr->push_back(block_key);
        // blocks_idx_ptr->push_back(j);
        read_treads.push_back(std::thread(getFromNode, expect_block_number, block_idx, block_key, block_size, ip_and_port.first, ip_and_port.second));
      }
      for (int j = 0; j < all_expect_blocks; j++)
      {
        read_treads[j].detach();
        // read_treads[j].join();
      }

      std::unique_lock<std::mutex> lck(*myLock_ptr);
      while (!check_received_block(k, expect_block_number, blocks_idx_ptr, blocks_ptr->size()))
      {
        cv_ptr->wait(lck);
      }
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "ready to decode!" << std::endl;
      }
      for (int j = 0; j < int(blocks_idx_ptr->size()); j++)
      {
        int idx = (*blocks_idx_ptr)[j];
        if (idx < k)
        {
          memcpy(data[idx], (*blocks_ptr)[j].data(), block_size);
        }
        else
        {
          memcpy(coding[idx - k], (*blocks_ptr)[j].data(), block_size);
        }
      }

      auto erasures = std::make_shared<std::vector<int>>();
      for (int j = 0; j < all_expect_blocks; j++)
      {
        if (std::find(blocks_idx_ptr->begin(), blocks_idx_ptr->end(), j) == blocks_idx_ptr->end())
        {
          erasures->push_back(j);
        }
      }
      erasures->push_back(-1);
      if (encode_type == Azure_LRC)
      {
        if (!decode(k, g_m, l, data, coding, erasures, block_size, encode_type))
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][GET] proxy cannot decode!" << std::endl;
        }
      }
      else
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET] proxy decode error!" << std::endl;
      }
      std::string value;
      for (int j = 0; j < k; j++)
      {
        value += std::string(data[j]);
      }

      if (IF_DEBUG)
      {
        std::cout << "\033[1;31m[Proxy" << m_self_cluster_id << "][GET]"
                  << "send " << key << " to client with length of " << value.size() << "\033[0m" << std::endl;
      }

      // send to the client
      asio::error_code error;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(clientip, std::to_string(clientport));
      asio::ip::tcp::socket sock_data(io_context);
      asio::connect(sock_data, endpoints);

      asio::write(sock_data, asio::buffer(key, key.size()), error);
      asio::write(sock_data, asio::buffer(value, value_size_bytes), error);
      asio::error_code ignore_ec;
      sock_data.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
      sock_data.close(ignore_ec);
    };
    try
    {
      // std::cerr << "decode_and_get_thread start" << std::endl;
      if (IF_DEBUG)
      {
        std::cout << "[Proxy] Handle get and decode" << std::endl;
      }
      std::thread my_thread(decode_and_get);
      my_thread.detach();
      // std::cerr << "decode_and_get_thread detach" << std::endl;
    }
    catch (std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cout << e.what() << std::endl;
    }

    return grpc::Status::OK;
  }

  // delete
  grpc::Status ProxyImpl::deleteBlock(
      grpc::ServerContext *context,
      const proxy_proto::NodeAndBlock *node_and_block,
      proxy_proto::DelReply *response)
  {
    std::vector<std::string> blocks_id;
    std::vector<std::string> nodes_ip_port;
    std::string key = node_and_block->key();
    int stripe_id = node_and_block->stripe_id();
    for (int i = 0; i < node_and_block->blockkeys_size(); i++)
    {
      blocks_id.push_back(node_and_block->blockkeys(i));
      std::string ip_port = node_and_block->datanodeip(i) + ":" + std::to_string(node_and_block->datanodeport(i));
      nodes_ip_port.push_back(ip_port);
    }
    auto delete_blocks = [this, key, blocks_id, stripe_id, nodes_ip_port]() mutable
    {
      auto request_and_delete = [this](std::string block_key, std::string node_ip_port)
      {
        bool ret = DelInDatanode(block_key, node_ip_port);
        if (!ret)
        {
          std::cout << "Delete value no return!" << std::endl;
          return;
        }
      };
      try
      {
        std::vector<std::thread> senders;
        for (int j = 0; j < int(blocks_id.size()); j++)
        {
          senders.push_back(std::thread(request_and_delete, blocks_id[j], nodes_ip_port[j]));
        }

        for (int j = 0; j < int(senders.size()); j++)
        {
          senders[j].join();
        }

        if (stripe_id != -1 || key != ""){
          grpc::ClientContext c_context;
          coordinator_proto::CommitAbortKey commit_abort_key;
          coordinator_proto::ReplyFromCoordinator rep;
          ECProject::OpperateType opp = DEL;
          commit_abort_key.set_opp(opp);
          commit_abort_key.set_key(key);
          commit_abort_key.set_ifcommitmetadata(true);
          commit_abort_key.set_stripe_id(stripe_id);
          grpc::Status stat;
          stat = m_coordinator_ptr->reportCommitAbort(&c_context, commit_abort_key, &rep);
        }
      }
      catch (const std::exception &e)
      {
        std::cout << "exception" << std::endl;
        std::cerr << e.what() << '\n';
      }
    };
    try
    {
      std::thread my_thread(delete_blocks);
      my_thread.detach();
    }
    catch (std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cout << e.what() << std::endl;
    }

    return grpc::Status::OK;
  }

  // lrcwidestripe, merge
  // parity block recalculation

  grpc::Status ProxyImpl::mainRecal(
      grpc::ServerContext *context,
      const proxy_proto::mainRecalPlan *main_recal_plan,
      proxy_proto::RecalReply *response)
  {
      int g_m, group_id, new_parity_num;
      bool if_partial_decoding;
      bool if_g_recal = main_recal_plan->type();
      int block_size = main_recal_plan->block_size();
      int stripe_id = main_recal_plan->stripe_id();
      int k = main_recal_plan->k();
      ECProject::EncodeType encode_type = (ECProject::EncodeType)main_recal_plan->encodetype();
      std::string recal_type = "";
      // for parity blocks
      std::vector<std::string> p_datanode_ip;
      std::vector<int> p_datanode_port;
      std::vector<std::string> p_blockkeys;
      // for help clusters
      std::vector<proxy_proto::locationInfo> help_locations;
      // for blocks in local datanodes
      std::vector<std::string> l_datanode_ip;
      std::vector<int> l_datanode_port;
      std::vector<std::string> l_blockkeys;
      std::vector<int> l_blockids;
      // get the meta information
      for (int i = 0; i < main_recal_plan->p_blockkeys_size(); i++)
      {
        p_datanode_ip.push_back(main_recal_plan->p_datanodeip(i));
        p_datanode_port.push_back(main_recal_plan->p_datanodeport(i));
        p_blockkeys.push_back(main_recal_plan->p_blockkeys(i));
      }
      if_partial_decoding = main_recal_plan->if_partial_decoding();
      if (!if_g_recal)
      {
        m_mutex.lock();
        m_merge_step_processing[1] = true;
        m_mutex.unlock();
        group_id = main_recal_plan->group_id();
        new_parity_num = 1;
        recal_type = "[Local]";
      }
      else if (if_g_recal)
      {
        m_mutex.lock();
        m_merge_step_processing[0] = true;
        m_mutex.unlock();
        g_m = main_recal_plan->g_m();
        new_parity_num = g_m;
        recal_type = "[Global]";
      }
      for (int i = 0; i < main_recal_plan->clusters_size(); i++)
      {
        if (int(main_recal_plan->clusters(i).cluster_id()) != m_self_cluster_id)
        {
          proxy_proto::locationInfo temp;
          temp.set_cluster_id(main_recal_plan->clusters(i).cluster_id());
          temp.set_proxy_ip(main_recal_plan->clusters(i).proxy_ip());
          temp.set_proxy_port(main_recal_plan->clusters(i).proxy_port());
          for (int j = 0; j < main_recal_plan->clusters(i).blockkeys_size(); j++)
          {
            temp.add_blockids(main_recal_plan->clusters(i).blockids(j));
            temp.add_blockkeys(main_recal_plan->clusters(i).blockkeys(j));
            temp.add_datanodeip(main_recal_plan->clusters(i).datanodeip(j));
            temp.add_datanodeport(main_recal_plan->clusters(i).datanodeport(j));
          }
          help_locations.push_back(temp);
        }
        else
        {
          for (int j = 0; j < main_recal_plan->clusters(i).blockkeys_size(); j++)
          {
            l_blockids.push_back(main_recal_plan->clusters(i).blockids(j));
            l_blockkeys.push_back(main_recal_plan->clusters(i).blockkeys(j));
            l_datanode_ip.push_back(main_recal_plan->clusters(i).datanodeip(j));
            l_datanode_port.push_back(main_recal_plan->clusters(i).datanodeport(j));
          }
        }
      }
      
      try
      {
        auto lock_ptr = std::make_shared<std::mutex>();
        auto blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
        auto blocks_key_ptr = std::make_shared<std::vector<std::string>>();
        auto blocks_idx_ptr = std::make_shared<std::vector<int>>();
        auto getFromNode = [this, blocks_ptr, blocks_key_ptr, blocks_idx_ptr, lock_ptr](int block_idx, std::string block_key, int block_size, std::string node_ip, int node_port) mutable
        {
          std::vector<char> temp(block_size);
          bool ret = GetFromDatanode(block_key.c_str(), block_key.size(), temp.data(), block_size, node_ip.c_str(), node_port, block_idx + 2);
          if (!ret)
          {
            std::cout << "getFromNode !ret" << std::endl;
            return;
          }
          lock_ptr->lock();
          blocks_ptr->push_back(temp);
          blocks_key_ptr->push_back(block_key);
          blocks_idx_ptr->push_back(block_idx);
          lock_ptr->unlock();
        };

        auto p_lock_ptr = std::make_shared<std::mutex>();
        auto m_blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
        auto m_blocks_idx_ptr = std::make_shared<std::vector<int>>();
        auto h_blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
        auto h_blocks_idx_ptr = std::make_shared<std::vector<int>>();
        auto getFromProxy = [this, recal_type, p_lock_ptr, m_blocks_ptr, m_blocks_idx_ptr, h_blocks_ptr, h_blocks_idx_ptr, block_size, if_partial_decoding, new_parity_num](int block_key_size, std::shared_ptr<asio::ip::tcp::socket> socket_ptr) mutable
        {
          try
          {
            asio::error_code ec;
            std::vector<unsigned char> int_buf(sizeof(int));
            asio::read(*socket_ptr, asio::buffer(int_buf, int_buf.size()), ec);
            int t_cluster_id = ECProject::bytes_to_int(int_buf);
            if (IF_DEBUG)
            {
              std::cout << "\033[1;36m" << recal_type << "[Main Proxy " << m_self_cluster_id << "] Try to get data from the proxy in cluster " << t_cluster_id << "\033[0m" << std::endl;
            }
            if (if_partial_decoding)
            {
              p_lock_ptr->lock();
              for (int j = 0; j < new_parity_num; j++)
              {
                std::vector<char> tmp_val(block_size);
                asio::read(*socket_ptr, asio::buffer(tmp_val.data(), block_size), ec);
                m_blocks_ptr->push_back(tmp_val);
              }
              m_blocks_idx_ptr->push_back(t_cluster_id);
              p_lock_ptr->unlock();
            }
            else
            {
              std::vector<unsigned char> int_buf_num_of_blocks(sizeof(int));
              asio::read(*socket_ptr, asio::buffer(int_buf_num_of_blocks, int_buf_num_of_blocks.size()), ec);
              int block_num = ECProject::bytes_to_int(int_buf_num_of_blocks);
              for (int j = 0; j < block_num; j++)
              {
                // std::vector<char> tmp_key(block_key_size);
                std::vector<char> tmp_val(block_size);
                std::vector<unsigned char> byte_block_id(sizeof(int));
                asio::read(*socket_ptr, asio::buffer(byte_block_id, byte_block_id.size()), ec);
                int block_idx = ECProject::bytes_to_int(byte_block_id);
                // asio::read(*socket_ptr, asio::buffer(tmp_key.data(), block_key_size), ec);
                asio::read(*socket_ptr, asio::buffer(tmp_val.data(), block_size), ec);
                p_lock_ptr->lock();
                h_blocks_ptr->push_back(tmp_val);
                h_blocks_idx_ptr->push_back(block_idx);
                p_lock_ptr->unlock();
              }
            }

            if (IF_DEBUG)
            {
              std::cout << "\033[1;36m" << recal_type << "[Main Proxy " << m_self_cluster_id << "] Finish getting data from the proxy in cluster " << t_cluster_id << "\033[0m" << std::endl;
            }
          }
          catch (const std::exception &e)
          {
            std::cerr << e.what() << '\n';
          }
        };

        auto send_to_datanode = [this](int j, std::string block_key, char *data, int block_size, std::string s_node_ip, int s_node_port)
        {
          SetToDatanode(block_key.c_str(), block_key.size(), data, block_size, s_node_ip.c_str(), s_node_port, j + 2);
        };

        if (IF_DEBUG)
        {
          std::cout << recal_type << "[Main Proxy" << m_self_cluster_id << "] get blocks in local cluster!" << std::endl;
        }
        // get data blocks in local cluster
        int l_block_num = int(l_blockkeys.size());
        if (l_block_num > 0)
        {
          try
          {
            std::vector<std::thread> read_threads;
            for (int j = 0; j < l_block_num; j++)
            {
              read_threads.push_back(std::thread(getFromNode, j, l_blockkeys[j], block_size, l_datanode_ip[j], l_datanode_port[j]));
            }
            for (int j = 0; j < l_block_num; j++)
            {
              read_threads[j].join();
            }
          }
          catch (const std::exception &e)
          {
            std::cerr << e.what() << '\n';
          }
          if (l_block_num != int(blocks_ptr->size()))
          {
            std::cout << "[Help] can't get enough blocks!" << std::endl;
          }
          std::vector<char *> v_data(l_block_num);
          std::vector<char *> v_coding(1);
          char **data = (char **)v_data.data();
          char **coding = (char **)v_coding.data();
          std::vector<std::vector<char>> v_data_area(l_block_num, std::vector<char>(block_size));
          for (int j = 0; j < l_block_num; j++)
          {
            data[j] = v_data_area[j].data();
          }
          for (int j = 0; j < l_block_num; j++)
          {
            int idx = (*blocks_idx_ptr)[j];
            if (idx < l_block_num)
            {
              memcpy(data[idx], (*blocks_ptr)[j].data(), block_size);
            }
          }
          if (if_partial_decoding) // partial encoding
          {
            std::vector<std::vector<char>> v_coding_area(new_parity_num, std::vector<char>(block_size));
            for (int j = 0; j < new_parity_num; j++)
            {
              coding[j] = v_coding_area[j].data();
            }
            if(if_g_recal)
            {
              encode_partial_blocks_for_gr(k, new_parity_num, data, coding, block_size, blocks_idx_ptr, l_block_num, encode_type);
            }
            else
            {
              perform_addition(data, coding, block_size, l_block_num, new_parity_num);
            }
            
            p_lock_ptr->lock();
            for (int j = 0; j < new_parity_num; j++)
            {
              m_blocks_ptr->push_back(v_coding_area[j]);
            }
            m_blocks_idx_ptr->push_back(m_self_cluster_id);
            p_lock_ptr->unlock();
          }
          else
          {
            for (int j = 0; j < l_block_num; j++)
            {
              p_lock_ptr->lock();
              h_blocks_idx_ptr->push_back((*blocks_idx_ptr)[j]);
              h_blocks_ptr->push_back((*blocks_ptr)[j]);
              p_lock_ptr->unlock();
            }
          }
        }

        // get from proxy
        int m_num = int(help_locations.size());
        if (IF_DEBUG)
        {
          std::cout << recal_type << "[Main Proxy" << m_self_cluster_id << "] get data blocks from " << m_num << " helper proxy!" << std::endl;
        }
        try
        {
          std::vector<std::thread> read_p_threads;
          for (int j = 0; j < m_num; j++)
          {
            int t_blocks_num = help_locations[j].blockkeys_size();
            int block_key_size = help_locations[0].blockkeys(0).size();
            if (if_partial_decoding)
            {
              block_key_size = 0;
              t_blocks_num = 1;
            }
            std::shared_ptr<asio::ip::tcp::socket> socket_ptr = std::make_shared<asio::ip::tcp::socket>(io_context);
            acceptor.accept(*socket_ptr);
            read_p_threads.push_back(std::thread(getFromProxy, block_key_size, socket_ptr));
            if (!if_partial_decoding)
            {
              l_block_num += t_blocks_num;
            }
            if (IF_DEBUG)
            {
              std::cout << recal_type << "[Main Proxy" << m_self_cluster_id << "] cluster" << help_locations[j].cluster_id() << " block_key_size:" << block_key_size << " blocks_num:" << help_locations[j].blockkeys_size() << std::endl;
            }
          }
          for (int j = 0; j < m_num; j++)
          {
            read_p_threads[j].join();
          }
        }
        catch (const std::exception &e)
        {
          std::cerr << e.what() << '\n';
        }
        if (l_block_num > 0)
        {
          m_num += 1; // add local
        }
        if (IF_DEBUG)
        {
          std::cout << recal_type << "[Main Proxy" << m_self_cluster_id << "] recalculating new parity blocks!" << std::endl;
        }
        // encode
        int count = l_block_num;
        if (if_partial_decoding)
        {
          count = m_num * new_parity_num;
        }
        std::vector<char *> vt_data(count);
        std::vector<char *> vt_coding(new_parity_num);
        char **t_data = (char **)vt_data.data();
        char **t_coding = (char **)vt_coding.data();
        std::vector<std::vector<char>> vt_data_area(count, std::vector<char>(block_size));
        std::vector<std::vector<char>> vt_coding_area(new_parity_num, std::vector<char>(block_size));
        if (IF_DEBUG)
        {
          std::cout << recal_type << "[Main Proxy" << m_self_cluster_id << "] " << count << " " << m_blocks_ptr->size() << " " << h_blocks_ptr->size() << std::endl;
        }
        for (int j = 0; j < count; j++)
        {
          t_data[j] = vt_data_area[j].data();
        }
        for (int j = 0; j < new_parity_num; j++)
        {
          t_coding[j] = vt_coding_area[j].data();
        }
        if (if_partial_decoding)
        {
          if(if_g_recal)
          {
            int index = 0;
            for(int j = 0; j < m_num; j++)
            {
              for(int jj = 0; jj < new_parity_num; jj++)
              {
                memcpy(t_data[index++], (*m_blocks_ptr)[jj * new_parity_num + j].data(), block_size);
              }
            }
          }
          else
          {
            for(int j = 0; j < count; j++)
            {
              memcpy(t_data[j], (*m_blocks_ptr)[j].data(), block_size);
            }
          }
        }
        else
        {
          for (int j = 0; j < count; j++)
          {
            memcpy(t_data[j], (*h_blocks_ptr)[j].data(), block_size);
          }
        }
        // clear
        blocks_ptr->clear();
        blocks_key_ptr->clear();
        blocks_idx_ptr->clear();
        m_blocks_ptr->clear();
        h_blocks_idx_ptr->clear();
        m_blocks_idx_ptr->clear();
        h_blocks_ptr->clear();
        
        if (IF_DEBUG)
        {
          std::cout << recal_type << "[Main Proxy" << m_self_cluster_id << "] encoding!" << std::endl;
        }
        try
        {
          if(if_partial_decoding || !if_g_recal)
          {
            perform_addition(t_data, t_coding, block_size, count, new_parity_num);
          }
          else
          {
            encode_partial_blocks_for_gr(k, g_m, t_data, t_coding, block_size, h_blocks_idx_ptr, count, encode_type);
          }
        }

        catch (const std::exception &e)
        {
          std::cerr << e.what() << '\n';
        }

        // set
        if (IF_DEBUG)
        {
          std::cout << recal_type << "[Main Proxy" << m_self_cluster_id << "] set new parity blocks!" << std::endl;
        }
        // int p_block_num = int(p_blockkeys.size());
        try
        {
          std::vector<std::thread> set_threads;
          for (int i = 0; i < new_parity_num; i++)
          {
            std::string new_id = "";
            if (if_g_recal){
              new_id = "Stripe" + std::to_string(stripe_id) + "_G" + std::to_string(i);
            }else{
              new_id = "Stripe" + std::to_string(stripe_id) + "_L" + std::to_string(group_id);
            }
            std::string s_node_ip = p_datanode_ip[i];
            int s_node_port = p_datanode_port[i];
            if (IF_DEBUG)
            {
              std::cout << recal_type << "[Main Proxy" << m_self_cluster_id << "] set " << new_id << " to datanode " << s_node_port << std::endl;
            }
            set_threads.push_back(std::thread(send_to_datanode, i, new_id, t_coding[i], block_size, s_node_ip, s_node_port));
          }
          for (int i = 0; i < new_parity_num; i++)
          {
            set_threads[i].join();
          }
        }
        catch (const std::exception &e)
        {
          std::cerr << e.what() << '\n';
        }
        if (if_g_recal){
          m_merge_step_processing[0] = false;
          cv.notify_all();
        }else{
          m_merge_step_processing[1] = false;
          cv.notify_all();
        }
      }
      catch (const std::exception &e)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "] error!" << std::endl;
        std::cerr << e.what() << '\n';
      }

      return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::helpRecal(
      grpc::ServerContext *context,
      const proxy_proto::helpRecalPlan *help_recal_plan,
      proxy_proto::RecalReply *response)
  {
    bool if_partial_decoding = help_recal_plan->if_partial_decoding();
    bool if_g_recal = help_recal_plan->type();
    std::string proxy_ip = help_recal_plan->mainproxyip();
    int proxy_port = help_recal_plan->mainproxyport();
    int block_size = help_recal_plan->block_size();
    int parity_num = help_recal_plan->parity_num();
    int k = help_recal_plan->k();
    ECProject::EncodeType encode_type = (ECProject::EncodeType)help_recal_plan->encodetype();
    std::vector<std::string> datanode_ip;
    std::vector<int> datanode_port;
    std::vector<std::string> blockkeys;
    std::vector<int> blockids;
    for (int i = 0; i < help_recal_plan->blockkeys_size(); i++)
    {
      datanode_ip.push_back(help_recal_plan->datanodeip(i));
      datanode_port.push_back(help_recal_plan->datanodeport(i));
      blockkeys.push_back(help_recal_plan->blockkeys(i));
      blockids.push_back(help_recal_plan->blockids(i));
    }
    
    // get data from the datanode
    auto myLock_ptr = std::make_shared<std::mutex>();
    auto blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
    auto blocks_key_ptr = std::make_shared<std::vector<std::string>>();
    auto blocks_idx_ptr = std::make_shared<std::vector<int>>();
    auto getFromNode = [this, blocks_ptr, blocks_key_ptr, blocks_idx_ptr, myLock_ptr](int block_idx, std::string block_key, int block_size, std::string node_ip, int node_port) mutable
    {
      std::vector<char> temp(block_size);
      bool ret = GetFromDatanode(block_key.c_str(), block_key.size(), temp.data(), block_size, node_ip.c_str(), node_port, block_idx + 2);

      if (!ret)
      {
        std::cout << "getFromNode !ret" << std::endl;
        return;
      }
      myLock_ptr->lock();
      blocks_ptr->push_back(temp);
      blocks_key_ptr->push_back(block_key);
      blocks_idx_ptr->push_back(block_idx);
      myLock_ptr->unlock();
    };
    if (IF_DEBUG)
    {
      std::cout << "[Helper Proxy" << m_self_cluster_id << "] Ready to read blocks from data node!" << std::endl;
    }
    int block_num = int(blockkeys.size());
    try
    {
      std::vector<std::thread> read_treads;
      for (int j = 0; j < block_num; j++)
      {
        read_treads.push_back(std::thread(getFromNode, blockids[j], blockkeys[j], block_size, datanode_ip[j], datanode_port[j]));
      }
      for (int j = 0; j < block_num; j++)
      {
        read_treads[j].join();
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }
    if (block_num != int(blocks_ptr->size()))
    {
      std::cout << "[Help] can't get enough blocks!" << std::endl;
    }
    std::vector<char *> v_data(block_num);
    std::vector<char *> v_coding(parity_num);
    char **data = (char **)v_data.data();
    char **coding = (char **)v_coding.data();
    std::vector<std::vector<char>> v_data_area(block_num, std::vector<char>(block_size));
    std::vector<std::vector<char>> v_coding_area(parity_num, std::vector<char>(block_size));
    for (int j = 0; j < block_num; j++)
    {
      data[j] = v_data_area[j].data();
    }
    for (int j = 0; j < parity_num; j++)
    {
      coding[j] = v_coding_area[j].data();
    }
    for (int j = 0; j < block_num; j++)
    {
      // int idx = (*blocks_idx_ptr)[j];
      // if (idx < block_num)
      // {
      //   memcpy(data[idx], (*blocks_ptr)[j].data(), block_size);
      // }
      memcpy(data[j], (*blocks_ptr)[j].data(), block_size);
    }

    // encode
    if (if_partial_decoding) // partial encoding
    {
      if (IF_DEBUG)
      {
        std::cout << "[Helper Proxy" << m_self_cluster_id << "] partial encoding!" << std::endl;
        for(auto it = blocks_idx_ptr->begin(); it != blocks_idx_ptr->end(); it++)
        {
            std::cout << (*it) << " ";
        }
        std::cout << std::endl;
      }
      
      if(if_g_recal)
      {
        encode_partial_blocks_for_gr(k, parity_num, data, coding, block_size, blocks_idx_ptr, block_num, encode_type);
      }
      else
      {
        perform_addition(data, coding, block_size, block_num, parity_num);
      }
    }

    // send to main proxy
    asio::error_code error;
    asio::io_context io_context;
    asio::ip::tcp::socket socket(io_context);
    asio::ip::tcp::resolver resolver(io_context);
    asio::error_code con_error;
    if (IF_DEBUG)
    {
      std::cout << "\033[1;36m[Helper Proxy" << m_self_cluster_id << "] Try to connect main proxy port " << proxy_port << "\033[0m" << std::endl;
    }
    asio::connect(socket, resolver.resolve({proxy_ip, std::to_string(proxy_port)}), con_error);
    if (!con_error && IF_DEBUG)
    {
      std::cout << "Connect to " << proxy_ip << ":" << proxy_port << " success!" << std::endl;
    }

    int value_size = 0;
    
    std::vector<unsigned char> int_buf_self_cluster_id = ECProject::int_to_bytes(m_self_cluster_id);
    asio::write(socket, asio::buffer(int_buf_self_cluster_id, int_buf_self_cluster_id.size()), error);
    if (!if_partial_decoding)
    {
      std::vector<unsigned char> int_buf_num_of_blocks = ECProject::int_to_bytes(block_num);
      asio::write(socket, asio::buffer(int_buf_num_of_blocks, int_buf_num_of_blocks.size()), error);
      int j = 0;
      for(auto it = blocks_idx_ptr->begin(); it != blocks_idx_ptr->end(); it++, j++)
      { 
        // send index and value
        int block_idx = *it;
        std::vector<unsigned char> byte_block_idx = ECProject::int_to_bytes(block_idx);
        asio::write(socket, asio::buffer(byte_block_idx, byte_block_idx.size()), error);
        asio::write(socket, asio::buffer(data[j], block_size), error);
        value_size += block_size;
      }
    }
    else
    {
      for (int j = 0; j < parity_num; j++)
      {
        asio::write(socket, asio::buffer(coding[j], block_size), error);
        value_size += block_size;
      }
    }
    asio::error_code ignore_ec;
    socket.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
    socket.close(ignore_ec);
    if (IF_DEBUG)
    {
      std::cout << "[Helper Proxy" << m_self_cluster_id << "] Send value to proxy" << proxy_port << "! With length of " << value_size << std::endl;
    }

    return grpc::Status::OK;
  }

  // block relocation
  // get -> set -> delete
  grpc::Status ProxyImpl::blockReloc(
      grpc::ServerContext *context,
      const proxy_proto::blockRelocPlan *reloc_plan,
      proxy_proto::blockRelocReply *response)
  {
    std::vector<std::string> blocks_id;
    std::vector<std::string> src_node_ip;
    std::vector<int> src_node_port;
    std::vector<std::string> des_node_ip;
    std::vector<int> des_node_port;
    int block_size = reloc_plan->block_size();
    for (int i = 0; i < reloc_plan->blocktomove_size(); i++)
    {
      blocks_id.push_back(reloc_plan->blocktomove(i));
      src_node_ip.push_back(reloc_plan->fromdatanodeip(i));
      src_node_port.push_back(reloc_plan->fromdatanodeport(i));
      des_node_ip.push_back(reloc_plan->todatanodeip(i));
      des_node_port.push_back(reloc_plan->todatanodeport(i));
    }
    auto relocate_blocks = [this, blocks_id, block_size, src_node_ip, src_node_port, des_node_ip, des_node_port]() mutable
    {
      auto relocate_single_block = [this](int j, std::string block_key, int block_size, std::string src_node_ip, int src_node_port, std::string des_node_ip, int des_node_port)
      {
        bool ret = BlockRelocation(block_key.c_str(), block_size, src_node_ip.c_str(), src_node_port, des_node_ip.c_str(), des_node_port);
        if(!ret)
        {
          std::cout << "[Block Relocation] Relocate " << block_key << " Failed!" << std::endl;
        }
        std::string src_ip_port = src_node_ip + ":" + std::to_string(src_node_port);
        bool ret3 = DelInDatanode(block_key, src_ip_port);
        if (!ret3)
        {
          std::cout << "[Block Relocation] Delete in the src node failed : " << block_key << std::endl;
        }
      };
      try
      {
        // std::vector<std::thread> senders;
        for (int j = 0; j < int(blocks_id.size()); j++)
        {
          relocate_single_block(j, blocks_id[j], block_size, src_node_ip[j], src_node_port[j], des_node_ip[j], des_node_port[j]);
          // senders.push_back(std::thread(relocate_single_block, j, blocks_id[j], block_size, src_node_ip[j], src_node_port[j], des_node_ip[j], des_node_port[j]));
        }
        // for (int j = 0; j < int(senders.size()); j++)
        // {
        //   senders[j].join();
        // }
        
        m_merge_step_processing[2] = false;
        cv.notify_all();
      }
      catch (const std::exception &e)
      {
        std::cout << "exception" << std::endl;
        std::cerr << e.what() << '\n';
      }
    };
    try
    {
      m_mutex.lock();
      m_merge_step_processing[2] = true;
      m_mutex.unlock();
      std::thread my_thread(relocate_blocks);
      my_thread.detach();
    }
    catch (std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cout << e.what() << std::endl;
    }
    return grpc::Status::OK;
  }

  // check
  grpc::Status ProxyImpl::checkStep(
      grpc::ServerContext *context,
      const proxy_proto::AskIfSuccess *step,
      proxy_proto::RepIfSuccess *response)
  {
    std::unique_lock<std::mutex> lck(m_mutex);
    int idx = step->step();
    if (IF_DEBUG)
    {
      std::cout << "\033[1;34m[Main Proxy" << m_self_cluster_id << "] Step" << idx << ":" << m_merge_step_processing[idx] << "\033[0m\n";
    }
    while (m_merge_step_processing[idx])
    {
      cv.wait(lck);
    }
    response->set_ifsuccess(true);
    return grpc::Status::OK;
  }

} // namespace ECProject
