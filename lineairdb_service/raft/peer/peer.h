#ifndef RAFT_PEER_H_
#define RAFT_PEER_H_

#include <string>
#include <cstdint>
#include "util/time.h"

#include "socket.h"

namespace Raft {

class Consensus;

class Peer {
public:
    Peer(Consensus& consensus, const std::string& ip, const std::string& port);
    ~Peer() = default;

    void beginLeadership();

    uint64_t getMatchIndex() const;

    Consensus& consensus_;
    Socket socket_;
    std::string ip_;
    std::string port_;

    // state
    bool exiting;
    bool request_vote_done;
    bool have_vote;
    bool is_caught_up;
    bool suppress_bulk_data;

    // log index
    uint64_t next_index;
    uint64_t match_index;
    uint64_t last_ack_epoch;

    TimePoint next_heartbeat_time;
    TimePoint backoff_until; 
    TimePoint last_rpc_send_time;
    TimePoint next_wakeup;
};

} // namespace Raft

#endif // RAFT_PEER_H_