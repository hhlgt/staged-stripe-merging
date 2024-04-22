#ifndef META_DEFINITION
#define META_DEFINITION
#include "devcommon.h"
namespace ECProject
{
  enum OpperateType
  {
    SET,
    GET,
    DEL,
    REPAIR,
    MERGE
  };
  enum EncodeType
  {
    Azure_LRC,
    Optimal_Cauchy_LRC
  };
  enum SingleStripePlacementType
  {
    Random,
    Flat,
    Optimal
  };
  enum MultiStripesPlacementType
  {
    Ran,
    DIS,
    AGG,
    OPT
  };

  typedef struct Block
  {
    int block_id;          // to denote the order of data blocks in a stripe
    std::string block_key; // to distinct block, globally unique
    char block_type;
    int block_size;
    int map2group, map2stripe, map2cluster, map2node;
    std::string map2key;
    Block(const std::string block_key, char block_type, int block_size, int map2group,
          int map2stripe, int map2cluster, int map2node, const std::string map2key)
        : block_key(block_key), block_type(block_type), block_size(block_size), map2group(map2group), map2stripe(map2stripe), map2cluster(map2cluster), map2node(map2node), map2key(map2key) {}
    Block() = default;
  } Block;

  typedef struct Cluster
  {
    int cluster_id;
    std::string proxy_ip;
    int proxy_port;
    std::vector<int> nodes;
    std::vector<Block *> blocks;
    std::unordered_set<int> stripes;
    Cluster(int cluster_id, const std::string &proxy_ip, int proxy_port) : cluster_id(cluster_id), proxy_ip(proxy_ip), proxy_port(proxy_port) {}
    Cluster() = default;
  } Cluster;

  typedef struct Node
  {
    int node_id;
    std::string node_ip;
    int node_port;
    int cluster_id;
    std::unordered_map<int, int> stripes;
    Node(int node_id, const std::string node_ip, int node_port, int cluster_id) : node_id(node_id), node_ip(node_ip), node_port(node_port), cluster_id(cluster_id) {}
    Node() = default;
  } Node;

  typedef struct Stripe
  {
    int k, l, g_m;
    int stripe_id;
    std::vector<std::string> object_keys;
    std::vector<int> object_sizes;
    std::vector<Block *> blocks;
    std::unordered_set<int> place2clusters;
  } Stripe;

  typedef struct ObjectInfo
  {
    int object_size;
    int map2stripe;
  } ObjectInfo;

  typedef struct ECSchema
  {
    ECSchema() = default;
    ECSchema(bool partial_decoding, EncodeType encodetype, SingleStripePlacementType s_stripe_placementtype,
             MultiStripesPlacementType m_stripe_placementtype, int k_datablock, int l_localparityblock, int g_m_globalparityblock,
             int b_datapergroup, int x_stripepermergegroup)
        : partial_decoding(partial_decoding), encodetype(encodetype), s_stripe_placementtype(s_stripe_placementtype),
          m_stripe_placementtype(m_stripe_placementtype), k_datablock(k_datablock), l_localparityblock(l_localparityblock),
          g_m_globalparityblock(g_m_globalparityblock), b_datapergroup(b_datapergroup), x_stripepermergegroup(x_stripepermergegroup) {}
    bool partial_decoding;
    EncodeType encodetype;
    SingleStripePlacementType s_stripe_placementtype;
    MultiStripesPlacementType m_stripe_placementtype;
    int k_datablock;
    int l_localparityblock;
    int g_m_globalparityblock;
    int b_datapergroup;
    int x_stripepermergegroup; // the product of xi
  } ECSchema;
} // namespace ECProject

#endif // META_DEFINITION