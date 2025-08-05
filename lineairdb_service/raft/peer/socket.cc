#include <unistd.h>
#include <arpa/inet.h>

#include "socket.h"
#include "util/debug.h"

namespace Raft {

Socket::Socket(const std::string& ip, const std::string& port) :
    ip_(ip),
    port_(port),
    socket_fd_(-1),
    connected_(false),
    address_(ip + ":" + port)
{}

Socket::~Socket() {
    if (socket_fd_ != -1) { 
        ::close(socket_fd_);
    }
}

bool Socket::connect() {
    if (connected_) return true;

    // create socket
    socket_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd_ < 0) {
        ERROR("Failed to create new TCP socket");
        return false;
    }

    // prepare server address struct
    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr(ip_.c_str());
    server_addr.sin_port = htons(std::stoi(port_));

    // try to connect
    if (::connect(socket_fd_, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        ::close(socket_fd_);
        socket_fd_ = -1;
        return false;
    }

    connected_ = true;
    INFO("Connected to %s", address_.c_str());
    return true;
}

bool Socket::isConnected() const {
    return connected_;
}

int Socket::getFd() const {
    return socket_fd_;
}

const std::string& Socket::getAddress() const {
    return address_;
}

} // namespace Raft