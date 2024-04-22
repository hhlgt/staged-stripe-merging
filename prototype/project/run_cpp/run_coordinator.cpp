#include "coordinator.h"

int main(int argc, char **argv)
{
  std::string coordinator_ip = "0.0.0.0";
  if (argc == 2)
  {
    coordinator_ip = std::string(argv[1]);
  }

  char buff[256];
  getcwd(buff, 256);
  std::string cwf = std::string(argv[0]);
  std::string config_path = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../config/clusterInformation.xml";
  std::cout << "Current working directory: " << config_path << std::endl;
  ECProject::Coordinator coordinator(coordinator_ip + ":55555", config_path);
  coordinator.Run();
  return 0;
}