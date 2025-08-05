#include "raft.h"
#include "util/util.h"
#include "consensus/consensus.h"
#include "util/debug.h"

namespace Raft {

RaftNode::RaftNode(const Options& options) :
    options_(options)
{
    server_id_ = Util::generateServerId(options_.ip, options_.port);
    ip_ = options_.ip;
    port_ = options_.port;
    address_ = options_.ip + ":" + options_.port;
}

void RaftNode::init() {
    if (!consensus) {
        consensus = std::make_shared<Consensus>(*this);
        consensus->init();
    }

    if (!rpc_server) {
        rpc_server = std::make_shared<RpcServer>(*this);
        rpc_server->init(options_.ip, options_.port);
        rpc_server->run();
    }

    if (!self_) {
        self_ = std::make_shared<Peer>(*consensus, options_.ip, options_.port);
    }

    // initialize servers
    for (const auto& server_address : Util::split(options_.peers, ',')) {
        // server_id
        std::vector<std::string> parts = Util::split(server_address, ':');
        std::string ip = parts[0];
        std::string port = parts[1];
        uint64_t server_id = Util::generateServerId(ip, port);

        peers_.emplace(server_id, Peer(*consensus, ip, port));
    }
}

void RaftNode::run() {
    consensus->run();
}

bool RaftNode::exiting() const {
    return exiting_;
}

uint64_t RaftNode::getServerId() const {
    return server_id_;
}

Peer* RaftNode::getPeer(uint64_t server_id) {
    // if server_id is self, return self
    if (server_id == server_id_) {
        return self_.get();
    }

    // check if server_id is in map
    auto it = peers_.find(server_id);
    if (it != peers_.end()) {
        return &(it->second);
    }

    ERROR("Peer with server_id %lu not found", server_id);
    return nullptr;
}

const std::string& RaftNode::getAddress() const {
    return address_;
}

const std::string& RaftNode::getPeerAddress(uint64_t server_id) const {
    // if server_id is self, return self address
    if (server_id == server_id_) {
        return getAddress();
    }
    
    // check if server_id is in the map
    auto it = peers_.find(server_id);
    if (it != peers_.end()) {
        return it->second.socket_.getAddress();
    }
    
    // if not in the map or special id (e.g., 0), return "-"
    static const std::string UNKNOWN = "-";
    return UNKNOWN;
}

} // namespace Raft