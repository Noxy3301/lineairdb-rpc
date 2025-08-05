#ifndef RAFT_SOCKET_H_
#define RAFT_SOCKET_H_

#include <string>

namespace Raft {

class Socket {
public:
    Socket(const std::string& ip, const std::string& port);
    ~Socket();

    bool connect();
    bool isConnected() const;
    
    int getFd() const;
    const std::string& getAddress() const;

private:    
    int socket_fd_;
    bool connected_;
    std::string ip_;
    std::string port_;
    std::string address_;
};

} // namespace Raft

#endif // RAFT_SOCKET_H_