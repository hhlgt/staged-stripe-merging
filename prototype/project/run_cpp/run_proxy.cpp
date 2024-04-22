#include "proxy.h"

int main(int argc, char **argv)
{
    std::string coordinator_ip = "0.0.0.0";
    if (argc == 3)
    {
        coordinator_ip = std::string(argv[2]);
    }
    pid_t pid = fork();
    if (pid > 0)
    {
        exit(0);
    }
    setsid();

    std::string ip_and_port(argv[1]);
    if (true)
    {
        umask(0);
        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
    }

    char buff[256];
    getcwd(buff, 256);
    std::string cwf = std::string(argv[0]);
    std::string config_path = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../config/clusterInformation.xml";
    // std::cout << "Current working directory: " << config_path << std::endl;
    ECProject::Proxy proxy(ip_and_port, config_path, coordinator_ip + ":55555");
    proxy.Run();
    return 0;
}