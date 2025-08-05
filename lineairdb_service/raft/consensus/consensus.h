#ifndef RAFT_CONSENSUS_H_
#define RAFT_CONSENSUS_H_

#include <chrono>

#include "raft.pb.h"
#include "storage/log.h"
#include "storage/layout.h"
#include "util/time.h"
#include "util/random.h"
#include "peer/peer.h"

namespace Raft {

class RaftNode;

class Consensus {
public:
    friend class Peer;

    // constructor and destructor
    Consensus(RaftNode& node);
    ~Consensus() = default;

    void init();
    void run();

    void printElectionState() const;
    void startNewElection();

    void append(const std::vector<const Protocol::Entry*>& entries);

    // Send RPC requests to other server
    void sendRequestVote(uint64_t server_id);
    void sendAppendEntries(uint64_t server_id);

    // Process RPC responses from other server
    void onRequestVote(uint64_t server_id, const Protocol::RequestVote::Response& response);
    void onAppendEntries(uint64_t server_id, const Protocol::AppendEntries::Response& response);

    // Process incoming RPC requests from other server
    void handleRequestVote(const Protocol::RequestVote::Request& request, Protocol::RequestVote::Response& response);
    void handleAppendEntries(const Protocol::AppendEntries::Request& request, Protocol::AppendEntries::Response& response);

private:
    enum class State {
        FOLLOWER,
        CANDIDATE,
        LEADER,
    };

    enum class QuorumField {
        MatchIndex,
    };

    uint64_t getLastLogTerm() const;
    void setElectionTimer();
    void stepDown(uint64_t new_term);
    void updateLogMetadata();

    bool hasQuorum() const;
    uint64_t quorumMin(QuorumField field) const;
    void becomeLeader();

    uint64_t packEntries(uint64_t next_index, Protocol::AppendEntries::Request& request) const;
    void advanceCommitIndex();

    RaftNode& node_;
    std::unique_ptr<Log> log_;
    StorageLayout storage_layout_;
    Xoroshiro128Plus rng_;

    const std::chrono::milliseconds ELECTION_TIMEOUT;
    const std::chrono::milliseconds HEARTBEAT_PERIOD;
    uint64_t MAX_LOG_ENTRIES_PER_REQUEST;

    uint64_t current_term_;
    State state_;
    uint64_t last_snapshot_index_;
    uint64_t last_snapshot_term_;
    // uint64_tlast_snapshot_cluster_time_;
    // uint64_t last_snapshot_bytes_;

    uint64_t commit_index_;
    uint64_t leader_id_;    // 0 means no leader for this term or does not know who it is
    uint64_t voted_for_;
    uint64_t current_epoch_;

    TimePoint startElectionAt_;
    TimePoint with_hold_votes_until_;

    uint64_t num_entries_truncated_;
};

} // namespace Raft

#endif // RAFT_CONSENSUS_H_