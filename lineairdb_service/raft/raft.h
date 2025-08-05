#ifndef RAFT_RAFT_H_
#define RAFT_RAFT_H_

#include <memory>
#include <map>
#include <unordered_map>

#include "consensus/consensus.h"
#include "peer/peer.h"
#include "rpc/rpc_server.h"
#include "util/options.h"

namespace Raft {

class Consensus;

class RaftNode {
public:
    friend class Consensus;
    friend class RpcServer;

    RaftNode(const Options& options);
    ~RaftNode() = default;

    void init();
    void run();

    bool exiting() const;

    uint64_t getServerId() const;
    Peer* getPeer(uint64_t server_id);
    const std::string& getAddress() const;
    const std::string& getPeerAddress(uint64_t server_id) const;

    std::shared_ptr<Consensus> consensus;
    std::shared_ptr<RpcServer> rpc_server;

private:
    std::unordered_map<uint64_t, Peer> peers_;
    std::shared_ptr<Peer> self_;

    uint64_t server_id_;
    std::string ip_;
    std::string port_;
    std::string address_;
    Options options_;

    bool exiting_ = false;
};

} // namespace Raft

#endif // RAFT_RAFT_H_