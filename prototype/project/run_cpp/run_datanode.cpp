#include "datanode.h"

int main(int argc, char **argv)
{
    pid_t pid = fork();
    if (pid > 0)
    {
        exit(0);
    }
    setsid();
    if (true)
    {
        umask(0);
        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
    }

    std::string ip_and_port(argv[1]);
    // std::string ip = ip_and_port.substr(0, ip_and_port.find(":"));
    // int port = std::stoi(ip_and_port.substr(ip_and_port.find(":") + 1, ip_and_port.size()));
    ECProject::DataNode datanode(ip_and_port);
    datanode.Run();
    return 0;
}
