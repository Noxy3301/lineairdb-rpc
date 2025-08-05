#include "consensus/consensus.h"
#include "raft.h"
#include "util/debug.h"
#include "rpc/message_header.h"
#include "util/util.h"

namespace Raft {

Consensus::Consensus(RaftNode& node)
    : node_(node)
    , storage_layout_()
    , log_()
    , current_term_(0)
    , state_(State::FOLLOWER)
    , ELECTION_TIMEOUT(std::chrono::milliseconds(500))
    , HEARTBEAT_PERIOD(std::chrono::milliseconds(250))
    , MAX_LOG_ENTRIES_PER_REQUEST(5000)
    , last_snapshot_index_(0)
    , last_snapshot_term_(0)
    , commit_index_(0)
    , leader_id_(0)
    , voted_for_(0)
    , current_epoch_(0)
    , startElectionAt_(TimePoint::max())
    , with_hold_votes_until_(TimePoint::min())
    , num_entries_truncated_(0)
{}

void Consensus::init() {
    // initialize storage layout
    if (storage_layout_.top_dir_.fd_ == -1) {
        storage_layout_.init(node_.ip_, node_.port_);
    }

    // initialize log
    if (!log_) {
        log_ = std::make_unique<Log>(storage_layout_.server_dir_);
    }

    // FIXME: Should restore from log, call stepDown(current_term_ which initialized by 0) for testing
    stepDown(current_term_);
}

void Consensus::run() {
    while (!node_.exiting_) {
        TimePoint now = Clock::now();

        // check election timer
        if (Clock::now() >= startElectionAt_) {
            startNewElection();
            continue;
        }

        for (auto& [server_id, peer] : node_.peers_) {
            // skip if in backoff period
            if (peer.backoff_until > now || peer.next_wakeup > now) {
                continue;
            }

            switch (state_) {
                // follower never sends RPCs
                case State::FOLLOWER:
                    peer.next_wakeup = TimePoint::max();
                    break;
                
                // candidate sends RequestVote if not received response yet (due to packet loss or smth)
                case State::CANDIDATE:
                    if (!peer.request_vote_done) {
                        sendRequestVote(server_id);
                    } else {
                        peer.next_wakeup = TimePoint::max();
                    }
                    break;

                // leader replicate entries and periodically sends heartbeat
                case State::LEADER:
                    if (peer.getMatchIndex() < log_->getLastLogIndex() || peer.next_heartbeat_time < now) {
                        sendAppendEntries(server_id);
                        peer.next_heartbeat_time = now + HEARTBEAT_PERIOD;
                    }
                    peer.next_wakeup = peer.next_heartbeat_time;
                    break;
            }
        }

        // TODO: CPU usage control?
    }
}

void Consensus::printElectionState() const {
    std::string state_str;
    switch (state_) {
        case State::FOLLOWER:
            state_str = "FOLLOWER";
            break;
        case State::CANDIDATE:
            state_str = "CANDIDATE";
            break;
        case State::LEADER:
            state_str = "LEADER";
            break;
    }
    DEBUG("server: %s, term: %lu, state: %s, leader: %s, voted_for: %s", 
        node_.getAddress().c_str(), current_term_, state_str.c_str(), 
        node_.getPeerAddress(leader_id_).c_str(), node_.getPeerAddress(voted_for_).c_str());
}

void Consensus::startNewElection() {
    if (leader_id_ > 0) {
        DEBUG("Starting election for term %lu (Leader %s not responding)", 
            current_term_ + 1, node_.getPeerAddress(leader_id_).c_str());
    } else if (state_ == State::CANDIDATE) {
        DEBUG("Restarting election for term %lu (previous election (term: %lu) timed out)", current_term_ + 1, current_term_);
    } else {
        DEBUG("Starting new election for term %lu", current_term_ + 1);
    }

    ++current_term_;
    state_ = State::CANDIDATE;
    leader_id_ = 0;
    voted_for_ = node_.getServerId();
    printElectionState();
    setElectionTimer();

    // send RequestVote RPC to all other peers
    for (auto& peer : node_.peers_) {
        sendRequestVote(peer.first);
    }
}

uint64_t Consensus::packEntries(uint64_t next_index, Protocol::AppendEntries::Request& request) const {
    uint64_t last_index = std::min(log_->getLastLogIndex(), next_index + MAX_LOG_ENTRIES_PER_REQUEST - 1);
    google::protobuf::RepeatedPtrField<Protocol::Entry>& request_entries = *request.mutable_entries();
    uint64_t num_entries = 0;

    for (uint64_t index = next_index; index <= last_index; ++index) {
        const Protocol::Entry& entry = log_->getEntry(index);
        *request_entries.Add() = entry;
        // TODO: add size-based optimization (see logcabin/Server/RaftConsensus.cc#L2607-L2625)
        ++num_entries;
    }

    assert(num_entries == static_cast<uint64_t>(request_entries.size()));
    return num_entries;
}

void Consensus::advanceCommitIndex() {
    if (state_ != State::LEADER) {
        WARNING("advanceCommitIndex() called in non-leader state, return");
        return;
    }

    // calcurate the largest log index that is replicated on a majority of servers
    uint64_t new_commit_index = quorumMin(QuorumField::MatchIndex);
    if (commit_index_ >= new_commit_index) {
        return;
    }

    // if we have discarded this entry, it must hagve been committed already (part of a snapshot)
    assert(new_commit_index >= log_->getLogStartIndex());

    // Safety: only log entries from the leader's current term are committed by counting replicas (§5.4.2)
    // at least one of these entries must also be from the current term to guarantee that no server without them can be elected
    if (log_->getEntry(new_commit_index).term() != current_term_) {
        return;
    }
    commit_index_ = new_commit_index;
    INFO("New commit index: %lu", commit_index_);
    assert(commit_index_ <= log_->getLastLogIndex());
    

    // TODO: handle configuration changes if needed
}

void Consensus::append(const std::vector<const Protocol::Entry*>& entries) {
    for (auto it = entries.begin(); it != entries.end(); ++it) {
        assert((*it)->term() != 0);
    }
    
    // MEMO: LogCabin separates sync/async on the state, but we always sync in append() for simplicity.
    std::pair<uint64_t, uint64_t> range = log_->append(entries);

    // TODO: configuration manager
}

void Consensus::sendRequestVote(uint64_t server_id) {
    // build up RequestVote request message
    Protocol::RequestVote::Request request;
    request.set_server_id(node_.getServerId());
    request.set_term(current_term_);
    request.set_last_log_term(getLastLogTerm());
    request.set_last_log_index(log_->getLastLogIndex());
    request.set_epoch(current_epoch_);

    // serialize message
    std::string serialized_message;
    if (!request.SerializeToString(&serialized_message)) {
        ERROR("Failed to serialize requestVote request message");
        return;
    }

    // get socket file descriptor from server id
    int fd = node_.rpc_server->getOrCreateSocketFd(server_id);
    if (fd < 0) {
        ERROR("Failed to establish connection to %s", node_.getPeerAddress(server_id).c_str());
        return;
    }

    // send message
    if (!node_.rpc_server->sendMessage(fd, server_id, MessageType::REQUEST_VOTE_REQUEST, serialized_message)) {
        ERROR("Failed to send requestVote request message to %s", node_.getPeerAddress(server_id).c_str());
        return;
    }
}

void Consensus::sendAppendEntries(uint64_t server_id) {
    Peer* peer = node_.getPeer(server_id);
    if (!peer) {
        ERROR("Peer %s not found", node_.getPeerAddress(server_id).c_str());
        return;
    }
    uint64_t last_log_index = log_->getLastLogIndex();
    uint64_t prev_log_index = peer->next_index - 1;
    assert(prev_log_index <= last_log_index);

    // if the follower is too far behind, send a snapshot instead of log entries
    if (peer->next_index < log_->getLogStartIndex()) {
        // installSnapshot();
        DEBUG("TODO: installSnapshot (return)");
        return;
    }

    // find prev_log_term, if not possible then send snapshot
    uint64_t prev_log_term;
    if (prev_log_index >= log_->getLogStartIndex()) {
        prev_log_term = log_->getEntry(prev_log_index).term();
    } else if (prev_log_index == 0) {
        prev_log_term = 0;
    } else {
        // if we can't determine prevLogTerm, send a snapshot
        // installSnapshot();
        DEBUG("TODO: installSnapshot (return)");
        return;
    }

    // build up AppendEntries request message
    Protocol::AppendEntries::Request request;
    request.set_server_id(node_.getServerId());
    request.set_term(current_term_);
    request.set_prev_log_term(prev_log_term);
    request.set_prev_log_index(prev_log_index);
    uint64_t num_entries = 0;
    if (!peer->suppress_bulk_data) {
        num_entries = packEntries(peer->next_index, request);
    }
    request.set_commit_index(std::min(commit_index_, prev_log_index + num_entries));
    request.set_epoch(current_epoch_);
    request.set_num_entries(num_entries);

    // serialize message
    std::string serialized_message;
    if (!request.SerializeToString(&serialized_message)) {
        ERROR("Failed to serialize AppendEntries request message");
        return;
    }

    // get socket file descriptor from server id
    int fd = node_.rpc_server->getOrCreateSocketFd(server_id);
    if (fd < 0) {
        ERROR("Failed to establish connection to %s", node_.getPeerAddress(server_id).c_str());
        return;
    }

    // send message
    peer->last_rpc_send_time = Clock::now();
    if (!node_.rpc_server->sendMessage(fd, server_id, MessageType::APPEND_ENTRIES_REQUEST, serialized_message)) {
        ERROR("Failed to send AppendEntries request message to %s", node_.getPeerAddress(server_id).c_str());
        return;
    }
}

void Consensus::onRequestVote(uint64_t server_id, const Protocol::RequestVote::Response& response) {
    Peer* peer = node_.getPeer(server_id);
    if (!peer) {
        ERROR("Peer %s not found", node_.getPeerAddress(server_id).c_str());
        return;
    }

    // check if the response is still valid
    if (state_ != State::CANDIDATE) {
        INFO("Received RequestVote response from %s:%s (term: %lu) but this server (term: %lu) is stale, ignore", 
            peer->ip_.c_str(), peer->port_.c_str(), response.term(), current_term_);
        return;
    }

    if (response.term() > current_term_) {
        INFO("Received RequestVote response from %s:%s (term: %lu) and this server (term: %lu) is behind, step down to follower", 
            peer->ip_.c_str(), peer->port_.c_str(), response.term(), current_term_);
        stepDown(response.term());
    } else {
        // update server state
        peer->request_vote_done = true;
        peer->last_ack_epoch = response.epoch();

        if (response.vote_granted()) {
            peer->have_vote = true;
            INFO("Got vote from %s:%s (term: %lu)", 
                peer->ip_.c_str(), peer->port_.c_str(), response.term());
            if (hasQuorum()) {
                becomeLeader();
            }
        } else {
            INFO("RequestVote rejected by %s:%s (term: %lu)", 
                peer->ip_.c_str(), peer->port_.c_str(), response.term());
        }
    }
}

void Consensus::onAppendEntries(uint64_t server_id, const Protocol::AppendEntries::Response& response) {
    Peer* peer = node_.getPeer(server_id);
    if (!peer) {
        ERROR("Peer %s not found", node_.getPeerAddress(server_id).c_str());
        return;
    }

    // check if the response is still valid
    if (state_ != State::LEADER) {
        INFO("Received AppendEntries response from %s:%s (term: %lu) but this server is not a leader, ignore", 
            peer->ip_.c_str(), peer->port_.c_str(), response.term(), current_term_);
        return;
    }

    if (response.term() > current_term_) {
        INFO("Received AppendEntries response from %s:%s (term: %lu) and this server (term: %lu) is behind, step down to follower",
            peer->ip_.c_str(), peer->port_.c_str(), response.term(), current_term_);
        stepDown(response.term());
    } else {
        assert(response.term() == current_term_);
        peer->last_ack_epoch = response.epoch();
        peer->next_heartbeat_time = peer->last_rpc_send_time + HEARTBEAT_PERIOD;

        if (response.success()) {
            if (peer->match_index > response.prev_log_index() + response.num_entries()) {
                // With pipelined AppendEntries (sending requests continuously without waiting for each response), responses may arrive out of order and matchIndex may decrease.
                WARNING("match_index should monotonically increase within a term since server don't forget entires, but it didn't.");
            } else {
                peer->match_index = response.prev_log_index() + response.num_entries();
                advanceCommitIndex();
            }
            peer->next_index = peer->match_index + 1;
            peer->suppress_bulk_data = false;

            // TODO: caught up check for availability

        } else {
            if (peer->next_index > 1) {
                --peer->next_index;
            }
            // if follower has a much shorter log, use its last_log_index to jump
            // directly to the right position instead of decrementing one by one
            if (response.has_last_log_index() && peer->next_index > response.last_log_index() + 1) {
                peer->next_index = response.last_log_index() + 1;
            }
        }
    }

    // TODO: state machine?

}

void Consensus::handleRequestVote(const Protocol::RequestVote::Request& request, Protocol::RequestVote::Response& response) {
    // TODO: mutexいる？
    // verify server is not exiting
    assert(!node_.exiting());

    // check if candidate's log is sufficiently up-to-date
    uint64_t last_log_index = log_->getLastLogIndex();
    uint64_t last_log_term = getLastLogTerm();
    // bool is_log_ok = (request.last_log_term() > last_log_term || 
    //                   (request.last_log_term() == last_log_term && request.last_log_index() >= last_log_index)); // for pre-vote extension

    // Skip voting if a message was recently received from a live leader
    if (with_hold_votes_until_ > Clock::now()) {
        INFO("Rejecting ReuqestVote request from %s (term: %lu) since this server (term: %lu) recently heard from a leader (%s)", 
            node_.rpc_server->getAddressFromServerId(request.server_id()).c_str(), 
            request.term(), 
            current_term_, 
            node_.rpc_server->getAddressFromServerId(leader_id_).c_str());
        response.set_epoch(request.epoch());
        response.set_term(current_term_);
        response.set_vote_granted(false);
        // response.set_log_ok(is_log_ok);  // for pre-vote extension
        return;
    }

    if (request.term() > current_term_) {
        INFO("Received ReqestVote request from %s (term: %lu) and this server (term: %lu) is behind, step down to follower", 
            node_.rpc_server->getAddressFromServerId(request.server_id()).c_str(), 
            request.term(), 
            current_term_);
        stepDown(request.term());
    }

    if (request.term() == current_term_) {
        if (voted_for_ == 0) {  // TODO: check is_log_ok for pre-vote extension
            INFO("Voting for %s (term: %lu)", node_.rpc_server->getAddressFromServerId(request.server_id()).c_str(), current_term_);
            stepDown(request.term());
            setElectionTimer();
            voted_for_ = request.server_id();
            updateLogMetadata();
            printElectionState();
        }
    }

    // Fill in response
    response.set_term(current_term_);
    response.set_vote_granted(request.term() == current_term_);
    response.set_epoch(request.epoch()); // for update last_ack_epoch
    // response.set_log_ok(is_log_ok);  // for pre-vote extension
}

void Consensus::handleAppendEntries(const Protocol::AppendEntries::Request& request, Protocol::AppendEntries::Response& response) {
    assert(!node_.exiting());

    // set default response values to reject, will override if accepted
    response.set_term(current_term_);
    response.set_success(false);
    response.set_last_log_index(log_->getLastLogIndex());
    
    // echoes request for correct matchIndex() update across RPC boundaries
    response.set_epoch(request.epoch());
    response.set_prev_log_index(request.prev_log_index());
    response.set_num_entries(request.entries_size());

    // TODO: piggy-back server capabilities in response (see logcabin/Server/RaftConsensus.cc#L1276-L1286)

    if (request.term() < current_term_) {
        INFO("Caller %s has stale term %lu (our term: %lu), reject",
            node_.rpc_server->getAddressFromServerId(request.server_id()).c_str(),
            request.term(), current_term_);
        return; // response was set to a rejection above
    }

    if (request.term() > current_term_) {
        INFO("Received AppendEntries request from %s (term %lu) and this server (term: %lu) is behind",
            node_.rpc_server->getAddressFromServerId(request.server_id()).c_str(),
            request.term(), current_term_);
        // update response term before our term changes in stepDown
        response.set_term(request.term());
    }

    // this request is a sign of life (heartbeat) from the current leader
    // - update our term and convert to follower if necessary
    // - reset election timer
    //   - set election timer here in case we exit the function early
    //   - election timer will be set again after the disk write
    stepDown(request.term());
    setElectionTimer();
    with_hold_votes_until_ = Clock::now() + ELECTION_TIMEOUT;

    // record the leader ID as a hint for clients
    if (leader_id_ == 0) {
        leader_id_ = request.server_id();
        INFO("Current Leader is %s (term: %lu)", 
            node_.rpc_server->getAddressFromServerId(leader_id_).c_str(), current_term_);
        printElectionState();
    } else {
        assert(leader_id_ == request.server_id());
    }

    // entries must be added in sequence
    if (request.prev_log_index() > log_->getLastLogIndex()) {
        INFO("Rejecting AppendEntries request from %s (term: %lu): missing log entries (request/prev_log_index: %lu, log/last_log_index: %lu)",
            node_.rpc_server->getAddressFromServerId(request.server_id()).c_str(),
            request.term(), request.prev_log_index(), log_->getLastLogIndex());
        return; // response was set to a rejection above
    }

    // validation successful, accepting request
    response.set_success(true);

    // handle duplicated/out-of-order AppendEntries requests by term comparison to prevent two issues:
    // 1. data loss when old requests arrive after new ones
    //   - e.g., leader appends entries 4,5, but follower receives 4,5,4
    //   - without this check, entry 5 would be truncated
    // 2. vulnerability during non-atomic truncate/append disk operation
    // (see logcabin/Server/RaftConsensus.cc#1340-L1355)
    uint64_t index = request.prev_log_index();
    for (auto it = request.entries().begin(); it != request.entries().end(); ++it) {
        ++index;
        const Protocol::Entry& entry = *it;

        // verify entry index matches our calculated index (see logcabin #160)
        if (entry.has_index()) {
            assert(entry.index() == index);
        }

        // already snapshotted and discarded this index, skip
        if (index < log_->getLogStartIndex()) {
            continue;
        }

        // check if we (as follower) have this index and truncate if terms don't match
        if (log_->getLastLogIndex() >= index) {
            // if we already have this index with same term, it's the same entry from current leader - skip
            if (log_->getEntry(index).term() == entry.term()) {
                continue;
            }

            // should never truncate committed entries
            assert(commit_index_ < index);
            uint64_t last_index_kept = index - 1;
            uint64_t num_truncating = log_->getLastLogIndex() - last_index_kept;
            INFO("Truncating %lu entries after %lu from the log", num_truncating, last_index_kept);
            num_entries_truncated_ += num_truncating;
            log_->truncateSuffix(last_index_kept);
            // TODO: configuration manager(truncate suffix?)
        }

        // append this and all following entries
        std::vector<const Protocol::Entry*> entries;
        do {
            const Protocol::Entry& entry = *it;
            if (entry.type() == Protocol::EntryType::UNKNOWN) {
                PANIC("Leader %lu sent unknown entry type (index %lu, term %lu). Server restart with newer code may help.", 
                      leader_id_, index, entry.term());
            }
            entries.push_back(&entry);
            ++it;
            ++index;
        } while (it != request.entries().end());
        append(entries);
        // TODO update cluster clock?
        break;
    }
    response.set_last_log_index(log_->getLastLogIndex());

    // update our commit index if leader's is higher (prevents decreasing in rare cases with new leader)
    if (commit_index_ < request.commit_index()) {
        commit_index_ = request.commit_index();
        assert(commit_index_ <= log_->getLastLogIndex());
        INFO("New commit index: %lu", commit_index_);
    }

    // reset election timer again here to avoid unnecessary timeout caused by our slow disk writes
    setElectionTimer();
    with_hold_votes_until_ = Clock::now() + ELECTION_TIMEOUT;
}

uint64_t Consensus::getLastLogTerm() const {
    uint64_t last_log_index = log_->getLastLogIndex();
    if (last_log_index >= log_->getLogStartIndex()) {
        return log_->getEntry(last_log_index).term();
    } else {
        assert(last_log_index == last_snapshot_index_);
        return last_snapshot_term_;
    }
}

void Consensus::setElectionTimer() {
    std::chrono::milliseconds duration(
        rng_.random_int(
            static_cast<uint64_t>(ELECTION_TIMEOUT.count()),
            static_cast<uint64_t>(ELECTION_TIMEOUT.count() * 2)));
    DEBUG("Will become candidate in %ld ms", duration.count());
    startElectionAt_ = Clock::now() + duration;
}

/**
 * Transition to follower state.
 * 
 * This function is called when:
 *   1. Receive an RPC request with newer term
 *   2. Receive an RPC response indicating that this server is stale
 *   3. Discover a current leader while a candidate
 * 
 * @note In case #3, new_term will be the same as current_term_
 * @note This function will set the election timer if this server was previously a leader
 */
void Consensus::stepDown(uint64_t new_term) {
    assert(current_term_ <= new_term);
    if (current_term_ < new_term) {
        DEBUG("stepDown(%lu)", new_term);
        current_term_ = new_term;
        leader_id_ = 0;
        voted_for_ = 0;
        updateLogMetadata();
        // TODO: configration周りの設定必要？
        state_ = State::FOLLOWER;
        printElectionState();
    } else {    // current_term_ == new_term
        if (state_ != State::FOLLOWER) {
            state_ = State::FOLLOWER;
            printElectionState();
        }
    }

    // update timer
    if (startElectionAt_ == TimePoint::max()) { // this server was a leader
        setElectionTimer();
    }
    if (with_hold_votes_until_ == TimePoint::max()) {
        with_hold_votes_until_ = TimePoint::min();
    }
    // TODO: interrupt?

    // TODO: if the leader disk thread is currently writing to disk, wait until it's done
    // TODO: process any queued log sync operations before stepping down to maintain data consistency
}

void Consensus::updateLogMetadata() {
    log_->raft_metadata_.set_current_term(current_term_);
    log_->raft_metadata_.set_voted_for(voted_for_);
    DEBUG("updateMetadata start (term: %lu, voted_for: %s)", current_term_, Util::getAddressFromServerId(voted_for_).c_str());
    log_->updateMetadata();
    DEBUG("updateMetadata end");
}

bool Consensus::hasQuorum() const {
    if (node_.peers_.empty()) {
        return true;
    }
    uint64_t nodes = node_.peers_.size() + 1; // +1 for this server
    uint64_t votes = 1; // this server votes for itself
    for (auto& peer : node_.peers_) {
        if (peer.second.have_vote) {
            votes++;
        }
    }
    return votes >= (nodes / 2 + 1);
}

// Returns the quorum minimum value for the given field among peers
// MEMO: why this return algebra ensures quorum minimum?
uint64_t Consensus::quorumMin(QuorumField field) const {
    if (node_.peers_.empty()) {
        return 0;
    }
    std::vector<uint64_t> values;
    // add this server's value
    switch (field) {
        case QuorumField::MatchIndex:
            values.push_back(log_->getLastLogIndex());
            break;
    }
    // add all peers' values
    for (auto& peer : node_.peers_) {
        switch (field) {
            case QuorumField::MatchIndex:
                values.push_back(peer.second.getMatchIndex());
                break;
        }
    }
    std::sort(values.begin(), values.end());
    return values.at((values.size() - 1) / 2);
}

void Consensus::becomeLeader() {
    assert(state_ == State::CANDIDATE);
    INFO("Now this server becomes leader for term %lu (appending no-op entry at index %lu)", 
         current_term_, log_->getLastLogIndex() + 1);

    // update state
    state_ = State::LEADER;
    leader_id_ = node_.getServerId();
    printElectionState();
    startElectionAt_ = TimePoint::max();
    with_hold_votes_until_ = TimePoint::max();

    // TODO: update cluster clock?

    // reset server states
    for (auto& peer : node_.peers_) {
        peer.second.beginLeadership();
    }

    Protocol::Entry entry;
    entry.set_term(current_term_);
    entry.set_type(Protocol::EntryType::NOOP);
    // FIXME: replace with a logical clock (like LogCabin's ClusterClock) for proper timestamp ordering in distributed systems
    entry.set_cluster_time(static_cast<uint64_t>(Clock::now().time_since_epoch().count()));
    append({&entry});
}

} // namespace Raft