#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "rpc_server.h"
#include "raft.h"
#include "util/debug.h"
#include "util/util.h"

namespace Raft {

RpcServer::RpcServer(RaftNode& node):
    node_(node),
    max_fd_(-1)
{
    FD_ZERO(&read_fds_);
}

void RpcServer::init(std::string ip, std::string port) {
    // create listen socket
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) { 
        FATAL("Failed to create new TCP socket");
    }

    // set socket options
    int reuse = 1;
    if (::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        ::close(listen_fd_);
        FATAL("Failed to set SO_REUSEADDR on socket");
    }

    // bind socket to local address
    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr(ip.c_str());
    server_addr.sin_port = htons(std::stoi(port));
    if (::bind(listen_fd_, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        ::close(listen_fd_);
        FATAL("Failed to bind socket to %s:%s", ip.c_str(), port.c_str());
    }

    // listen for incoming connections
    if (::listen(listen_fd_, 1024) < 0) {
        ::close(listen_fd_);
        FATAL("Failed to listen on socket");
    }

    // add listen socket to fd_set
    FD_SET(listen_fd_, &read_fds_);
    max_fd_ = listen_fd_;

    INFO("RPC server initialized (listening on %s:%s)", ip.c_str(), port.c_str());
}

void RpcServer::run() {
    rpc_server_thread_ = std::thread(&RpcServer::serverLoop, this);
}

void RpcServer::serverLoop() {
    while (!node_.exiting()) {
        // create a copy as select() modifies the fd_set, removing non-ready descriptors
        fd_set read_fds = read_fds_;

        // setup timeout (10ms)
        struct timeval timeout;
        timeout.tv_sec = 0;
        timeout.tv_usec = 10000;   // TODO: Set appropriate timeout

        // wait for events
        int activity = ::select(max_fd_ + 1, &read_fds, nullptr, nullptr, &timeout);
        if (activity < 0) {
            if (errno != EINTR) {
                ERROR("select() error: %s", strerror(errno));
            }
            continue;
        }

        // timeout
        if (activity == 0) {
            continue;
        }

        // check for active connections
        for (int fd = 0; fd <= max_fd_; ++fd) {
            if (!FD_ISSET(fd, &read_fds)) {
                continue;
            }

            if (fd == listen_fd_) {
                acceptNewConnection();
            } else {
                handleClientData(fd);
            }
        }
    }
}

void RpcServer::addReadFd(int fd) {
    FD_SET(fd, &read_fds_);
    if (fd > max_fd_) {
        max_fd_ = fd;
    }
}

int RpcServer::getOrCreateSocketFd(uint64_t server_id) {
    int fd = -1;

    for (auto& peer : node_.peers_) {
        if (peer.first == server_id) {
            fd = peer.second.socket_.getFd();

            // if socket is not connected, try to connect
            if (fd == -1 || !peer.second.socket_.isConnected()) {
                if (peer.second.socket_.connect()) {
                    fd = peer.second.socket_.getFd();
                    addReadFd(fd);
                }
            }
        }
    }

    return fd;
}

std::string RpcServer::getAddressFromFd(int fd) {
    std::string address = "";

    for (auto& peer : node_.peers_) {
        if (peer.second.socket_.getFd() == fd) {
            address = peer.second.socket_.getAddress();
            break;
        }
    }

    return address;
}

std::string RpcServer::getAddressFromServerId(uint64_t server_id) {
    std::string address = "";
    for (auto& peer : node_.peers_) {
        if (peer.first == server_id) {
            address = peer.second.socket_.getAddress();
            break;
        }
    }

    return address;
}

void RpcServer::acceptNewConnection() {
    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);

    int client_fd = ::accept(listen_fd_, (struct sockaddr*)&client_addr, &client_addr_len);
    if (client_fd < 0) {
        ERROR("Failed to accept new connection");
        return;
    }

    // add new client socket to fd_set
    FD_SET(client_fd, &read_fds_);
    if (client_fd > max_fd_) {
        max_fd_ = client_fd;
    }

    INFO("New connection established"); // 
}

void RpcServer::handleClientData(int fd) {
    // peek message header
    MessageHeader header;
    ssize_t header_read = ::recv(fd, &header, sizeof(header), MSG_PEEK);
    if (header_read <= 0) {
        if (header_read < 0) {
            ERROR("Failed to peek message header from %s", getAddressFromFd(fd).c_str());
        } else {    // header_read == 0
            ERROR("Client %s disconnected", getAddressFromFd(fd).c_str());
        }
        closeConnection(fd);
        return;
    }

    // if message header is not complete, read the rest of the message
    if (header_read < static_cast<ssize_t>(sizeof(header))) {
        INFO("Incomplete message header from %s (%zu/%zu bytes)", 
            getAddressFromFd(fd).c_str(), header_read, sizeof(header));
        return;
    }

    // convert message header (network order -> host order)
    uint64_t sender_id = be64toh(header.sender_id);
    MessageType message_type = static_cast<MessageType>(ntohl(header.message_type));
    uint32_t payload_size = ntohl(header.payload_size);

    // prepare buffer
    size_t total_size = sizeof(header) + payload_size;
    std::vector<char> buffer(total_size);

    // read message header and payload
    ssize_t total_read = 0;
    while (total_read < total_size) {
        ssize_t bytes_read = ::recv(fd, buffer.data() + total_read, total_size - total_read, 0);

        if (bytes_read <= 0) {
            if (bytes_read < 0) {
                ERROR("Failed to receive data from %s", getAddressFromFd(fd).c_str());
            } else {    // bytes_read == 0
                ERROR("Client %s disconnected", getAddressFromFd(fd).c_str());
            }
            closeConnection(fd);
            return;
        }

        total_read += bytes_read;
        if (total_read < total_size) {
            INFO("Partial message received from %s (%zu/%zu bytes)", 
                getAddressFromFd(fd).c_str(), total_read, total_size);
        }
    }

    INFO("Received message from %s (%zu bytes)", Util::getAddressFromServerId(sender_id).c_str(), total_size);

    // handle RPC
    std::string payload(buffer.data() + sizeof(header), payload_size);
    std::string result = "";
    handleRPC(sender_id, message_type, payload, result);
    if (!result.empty()) {
        switch (message_type) {
            case MessageType::REQUEST_VOTE_REQUEST:
                sendMessage(fd, sender_id, MessageType::REQUEST_VOTE_RESPONSE, result);
                break;
            case MessageType::APPEND_ENTRIES_REQUEST:
                sendMessage(fd, sender_id, MessageType::APPEND_ENTRIES_RESPONSE, result);
                break;
            default:
                // no need to send response
                break;
        }
    }
}

void RpcServer::closeConnection(int fd) {
    close(fd);
    FD_CLR(fd, &read_fds_);

    // update max_fd_ if necessary
    if (fd == max_fd_) {
        for (int i = max_fd_; i >= 0; --i) {
            if (FD_ISSET(i, &read_fds_)) {
                max_fd_ = i;
                break;
            }
        }
    }
}

void RpcServer::handleRPC(uint64_t sernder_id, MessageType message_type, const std::string& message, std::string& result) {
    switch(message_type) {
        case MessageType::REQUEST_VOTE_REQUEST:
            handleRequestVote(message, result);
            return;
        case MessageType::REQUEST_VOTE_RESPONSE:
            onRequestVote(sernder_id, message);
            return;
        case MessageType::APPEND_ENTRIES_REQUEST:
            handleAppendEntries(message, result);
            return;
        case MessageType::APPEND_ENTRIES_RESPONSE:
            onAppendEntries(sernder_id, message);
            return;
    }

    PANIC("Unknown message type: %d", message_type);
}

void RpcServer::handleRequestVote(const std::string& message, std::string& result) {
    Protocol::RequestVote_Request request;
    Protocol::RequestVote_Response response;
    request.ParseFromString(message);
    node_.consensus->handleRequestVote(request, response);
    result = response.SerializeAsString();
}

void RpcServer::onRequestVote(uint64_t sender_id, const std::string& message) {
    Protocol::RequestVote_Response response;
    response.ParseFromString(message);
    node_.consensus->onRequestVote(sender_id, response);
}

void RpcServer::handleAppendEntries(const std::string& message, std::string& result) {
    Protocol::AppendEntries::Request request;
    Protocol::AppendEntries::Response response;
    request.ParseFromString(message);
    node_.consensus->handleAppendEntries(request, response);
    result = response.SerializeAsString();
}

void RpcServer::onAppendEntries(uint64_t sender_id, const std::string& message) {
    Protocol::AppendEntries::Response response;
    response.ParseFromString(message);
    node_.consensus->onAppendEntries(sender_id, response);
}


bool RpcServer::sendMessage(int fd, uint64_t server_id, MessageType message_type, const std::string& message) {
    if (fd < 0) {
        ERROR("Invalid socket file descriptor: %d", fd);
        return false;
    }

    // prepare message header
    MessageHeader header;
    header.sender_id = htobe64(node_.getServerId());
    header.message_type = htonl(message_type);
    header.payload_size = htonl(static_cast<uint32_t>(message.size()));

    // combine header and message
    size_t total_size = sizeof(header) + message.size();
    std::vector<char> buffer(total_size);
    std::memcpy(buffer.data(), &header, sizeof(header));
    std::memcpy(buffer.data() + sizeof(header), message.c_str(), message.size());

    // send header and message
    ssize_t bytes_sent = ::send(fd, buffer.data(), total_size, 0);
    if (bytes_sent != static_cast<ssize_t>(total_size)) {
        ERROR("Failed to send message to %s", getAddressFromServerId(server_id).c_str());
        return false;
    }

    DEBUG("Successfully sent %d bytes to %s (%s)", total_size, getAddressFromServerId(server_id).c_str(), 
          message_type == MessageType::REQUEST_VOTE_REQUEST ? "RequestVote Request" : 
          message_type == MessageType::REQUEST_VOTE_RESPONSE ? "RequestVote Response" : 
          message_type == MessageType::APPEND_ENTRIES_REQUEST ? "AppendEntries Request" : 
          message_type == MessageType::APPEND_ENTRIES_RESPONSE ? "AppendEntries Response" : 
          "UNKNOWN");

    return true;
}

} // namespace Raft