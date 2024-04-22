#include "client.h"
#include "toolbox.h"
#include <fstream>
#include <sys/time.h>
#include <unistd.h>
#include <sys/stat.h>

int main(int argc, char **argv)
{
  if (argc != 13 && argc != 14)
  {
    std::cout << "./run_client partial_decoding encode_type singlestripe_placement_type multistripes_placement_type k l g_m stripe_num stage_x1 stage_x2 stage_x3 value_length" << std::endl;
    std::cout << "./run_client false Azure_LRC Optimal OPT 8 2 2 32 2 2 0 1024" << std::endl;
    exit(-1);
  }

  bool partial_decoding;
  ECProject::EncodeType encode_type;
  ECProject::SingleStripePlacementType s_placement_type;
  ECProject::MultiStripesPlacementType m_placement_type;
  int k, l, g_m, b;
  int stripe_num, value_length;
  int s_x1, s_x2, s_x3;

  char buff[256];
  getcwd(buff, 256);
  std::string cwf = std::string(argv[0]);

  partial_decoding = (std::string(argv[1]) == "true");
  if (std::string(argv[2]) == "Azure_LRC")
  {
    encode_type = ECProject::Azure_LRC;
  }
  else if(std::string(argv[2]) == "Optimal_Cauchy_LRC")
  {
    encode_type = ECProject::Optimal_Cauchy_LRC;
  }
  else
  {
    std::cout << "error: unknown encode_type" << std::endl;
    exit(-1);
  }
  if (std::string(argv[3]) == "Optimal")
  {
    s_placement_type = ECProject::Optimal;
  }
  else
  {
    std::cout << "error: unknown singlestripe_placement_type" << std::endl;
    exit(-1);
  }
  if (std::string(argv[4]) == "Ran")
  {
    m_placement_type = ECProject::Ran;
  }
  else if (std::string(argv[4]) == "DIS")
  {
    m_placement_type = ECProject::DIS;
  }
  else if (std::string(argv[4]) == "AGG")
  {
    m_placement_type = ECProject::AGG;
  }
  else if (std::string(argv[4]) == "OPT")
  {
    m_placement_type = ECProject::OPT;
  }
  else
  {
    std::cout << "error: unknown singlestripe_placement_type" << std::endl;
    exit(-1);
  }
  k = std::stoi(std::string(argv[5]));
  l = std::stoi(std::string(argv[6]));
  b = std::ceil((double)k / (double)l);
  g_m = std::stoi(std::string(argv[7]));
  stripe_num = std::stoi(std::string(argv[8]));
  s_x1 = std::stoi(std::string(argv[9]));
  s_x2 = std::stoi(std::string(argv[10]));
  s_x3 = std::stoi(std::string(argv[11]));
  value_length = std::stoi(std::string(argv[12]));

  std::string client_ip = "0.0.0.0", coordinator_ip;
  if (argc == 14)
  {
    coordinator_ip = std::string(argv[13]);
  }
  else
  {
    coordinator_ip = client_ip;
  }
  

  ECProject::Client client(client_ip, 44444, coordinator_ip + std::string(":55555"));
  std::cout << client.sayHelloToCoordinatorByGrpc("Client") << std::endl;

  int stage_num = 1;
  int x = s_x1;
  if (s_x1 == 0)
  {
    std::cout << "At least one stage of stripe merging!" << std::endl;
    exit(-1);
  }
  if (s_x2 != 0)
  {
    x *= s_x2;
    stage_num++;
  }
  if (s_x3 != 0)
  {
    x *= s_x3;
    stage_num++;
  }
  if (stripe_num > 100)
  {
    std::cout << "Do not support stripe number greater than 100!" << std::endl;
    exit(-1);
  }
  if (stripe_num % x != 0)
  {
    std::cout << "Stripe number not matches! stripe_num % (stage_x1 * stage_x2 * stage_x3) == 0 if stage_xi != 0." << std::endl;
    exit(-1);
  }

  if (client.SetParameterByGrpc({partial_decoding, encode_type, s_placement_type, m_placement_type, k, l, g_m, b, x}))
  {
    std::cout << "set parameter successfully!" << std::endl;
  }
  else
  {
    std::cout << "Failed to set parameter!" << std::endl;
  }

  std::unordered_map<std::string, std::string> key_values;

  // set
  std::cout << "[SET BEGIN]" << std::endl;
  for (int i = 0; i < stripe_num; i++)
  {
    std::string key;
    if (i < 10)
    {
      key = "Object0" + std::to_string(i);
    }
    else
    {
      key = "Object" + std::to_string(i);
    }
    std::string readpath = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../../data/" + key;
    // std::string readpath = "/mnt/e/erasure_codes/staged_stripe_merging/project/data/" + key;
    if (access(readpath.c_str(), 0) == -1)
    {
      std::cout << "[Client] file does not exist!" << std::endl;
      exit(-1);
    }
    // if (!std::filesystem::exists(std::filesystem::path{readpath}))
    // {
    //   std::cout << "[Read] file does not exist!" << readpath << std::endl;
    // }
    else
    {
      char *buf = new char[value_length * 1024];
      std::ifstream ifs(readpath);
      ifs.read(buf, value_length * 1024);
      client.set(key, std::string(buf));
      ifs.close();
      delete buf;
    }
  }
  std::cout << "[SET END]" << std::endl
            << std::endl;

  // std::cout << "[GET BEGIN]" << std::endl;
  // // get
  // for (int i = 0; i < stripe_num; i++)
  // {
  //   std::string value;
  //   std::string key = "Object" + std::to_string(i);
  //   std::string targetdir = "./client_get/";
  //   std::string writepath = targetdir + key;
  //   // if (!std::filesystem::exists(std::filesystem::path{"./client_get/"}))
  //   // {
  //   //   std::filesystem::create_directory("./client_get/");
  //   // }
  //   if (access(targetdir.c_str(), 0) == -1)
  //   {
  //     mkdir(targetdir.c_str(), S_IRWXU);
  //   }
  //   client.get(key, value);
  //   std::cout << "[run_client] value size " << value.size() << std::endl;
  //   std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
  //   ofs.write(value.c_str(), value.size());
  //   ofs.flush();
  //   ofs.close();
  // }
  // std::cout << "[GET END]" << std::endl
  //           << std::endl;

  // std::cout << "[DEL BEGIN]" << std::endl;
  // for (int i = 0; i < stripe_num; i++)
  // {
  //   std::string key = "Object" + std::to_string(i);
  //   client.delete_key(key);
  // }
  // std::cout << "[DEL END]" << std::endl;

  // merge
  std::cout << "Number of stripes(objects): " << stripe_num << std::endl;
  std::cout << "Object size: " << value_length << "KiB" << std::endl;
  std::cout << "Block size: " << float(value_length) / float(k) << "KiB" << std::endl;
  std::cout << "x (k, l, g): " << s_x1;
  if (s_x2 != 0)
    std::cout << " × " << s_x2;
  if (s_x3 != 0)
    std::cout << " × " << s_x3;
  std::cout << "(" << k << ", " << l << ", " << g_m << ")" << std::endl;
  if (m_placement_type == ECProject::Ran && partial_decoding)
  {
    std::cout << "Placement type: Ran-P" << std::endl;
  }
  else
  {
    std::cout << "Placement type: " << std::string(argv[4]) << std::endl;
  }
  std::cout << "[MERGE BEGIN]" << std::endl;
  double tot_time = 0.0, tot_cost = 0.0, temp_cost = 0.0;
  double s1_time = 0.0, s2_time = 0.0, s3_time = 0.0;
  struct timeval start_time, end_time;
  struct timeval s1_start_time, s1_end_time;
  struct timeval s2_start_time, s2_end_time;
  struct timeval s3_start_time, s3_end_time;
  gettimeofday(&start_time, NULL);

  gettimeofday(&s1_start_time, NULL);
  temp_cost = client.merge(s_x1);
  gettimeofday(&s1_end_time, NULL);
  s1_time = s1_end_time.tv_sec - s1_start_time.tv_sec + (s1_end_time.tv_usec - s1_start_time.tv_usec) * 1.0 / 1000000;
  std::cout << "Stage 1: " << temp_cost << std::endl;
  tot_cost += temp_cost;

  if (s_x2 != 0)
  {
    gettimeofday(&s2_start_time, NULL);
    temp_cost = client.merge(s_x2);
    gettimeofday(&s2_end_time, NULL);
    s2_time = s2_end_time.tv_sec - s2_start_time.tv_sec + (s2_end_time.tv_usec - s2_start_time.tv_usec) * 1.0 / 1000000;
    std::cout << "Stage 2: " << temp_cost << std::endl;
    tot_cost += temp_cost;
  }

  if (s_x3 != 0)
  {
    gettimeofday(&s3_start_time, NULL);
    temp_cost = client.merge(s_x3);
    gettimeofday(&s3_end_time, NULL);
    s3_time = s3_end_time.tv_sec - s3_start_time.tv_sec + (s3_end_time.tv_usec - s3_start_time.tv_usec) * 1.0 / 1000000;
    std::cout << "Stage 3: " << temp_cost << std::endl;
    tot_cost += temp_cost;
  }

  gettimeofday(&end_time, NULL);
  tot_time = end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

  std::cout << "Total Cost: " << tot_cost << std::endl;
  std::cout << "[MERGE END]" << std::endl
            << std::endl;

  std::cout << "[DEL BEGIN]" << std::endl;
  client.delete_all_stripes();
  std::cout << "[DEL END]" << std::endl
            << std::endl;
  return 0;
}