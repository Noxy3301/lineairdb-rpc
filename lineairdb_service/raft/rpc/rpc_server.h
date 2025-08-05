#ifndef RAFT_RPC_SERVER_H_
#define RAFT_RPC_SERVER_H_

#include <string>
#include <thread>
#include "message_header.h"

namespace Raft {

class RaftNode;

class RpcServer {
public:
    RpcServer(RaftNode& node);
    ~RpcServer() = default;

    void init(std::string ip, std::string port);
    void run();
    void serverLoop();

    void addReadFd(int fd);
    void removeReadFd(int fd);

    void acceptNewConnection();
    void handleClientData(int fd);
    void closeConnection(int fd);
    bool sendMessage(int fd, uint64_t server_id, MessageType message_type, const std::string& message);

    void handleRPC(uint64_t sender_id, MessageType message_type, const std::string& message, std::string& result);
    void handleRequestVote(const std::string& message, std::string& result);
    void onRequestVote(uint64_t sender_id, const std::string& message);
    void handleAppendEntries(const std::string& message, std::string& result);
    void onAppendEntries(uint64_t sender_id, const std::string& message);

    int getOrCreateSocketFd(uint64_t server_id);
    std::string getAddressFromFd(int fd);
    std::string getAddressFromServerId(uint64_t server_id);

private:
    int listen_fd_;
    RaftNode& node_;
    std::thread rpc_server_thread_;

    fd_set read_fds_;
    int max_fd_;
};

} // namespace Raft

#endif // RAFT_RPC_SERVER_H_