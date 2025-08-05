#include "peer.h"
#include "consensus/consensus.h"

namespace Raft {

Peer::Peer(Consensus& consensus, const std::string& ip, const std::string& port)
    : consensus_(consensus)
    , socket_(ip, port)
    , ip_(ip)
    , port_(port)
    , exiting(false)
    , request_vote_done(false)
    , have_vote(false)
    , suppress_bulk_data(true)
    , is_caught_up(false)
    , next_index(consensus.log_->getLastLogIndex() + 1)
    , match_index(0)
    , last_ack_epoch(0)
    , next_heartbeat_time(TimePoint::min())
    , backoff_until(TimePoint::min())
    , last_rpc_send_time(TimePoint::min())
    , next_wakeup(TimePoint::min())
{}

void Peer::beginLeadership() {
    next_index = consensus_.log_->getLastLogIndex() + 1;
    match_index = 0;
    suppress_bulk_data = true;
    // TODO: update snapshot index and offset

    next_heartbeat_time = Clock::now();
    next_wakeup = Clock::now(); // これも重要
}

uint64_t Peer::getMatchIndex() const {
    return match_index;
}

} // namespace Raft
