#ifndef RAFT_LOG_H_
#define RAFT_LOG_H_

#include <vector>
#include <cstdint>
#include <deque>

#include "raft.pb.h"
#include "metadata.pb.h"

#include "file.h"

namespace Raft {

class Log {
public:
    Log(const File& parent_dir);
    ~Log() = default;

    std::pair<uint64_t, uint64_t> append(const std::vector<const Protocol::Entry*>& new_entries);
    const Protocol::Entry& getEntry(uint64_t index) const;
    uint64_t getLogStartIndex() const;
    uint64_t getLastLogIndex() const;
    uint64_t getSizeBytes() const;
    std::string getName() const;

    // TODO: impl sync?

    void truncatePrefix(uint64_t first_index);
    void truncateSuffix(uint64_t last_index);
    void updateMetadata();
    File updateMetadataCallerSync();

    RaftMetadata raft_metadata_;
    LogMetadata log_metadata_;
    File log_dir_;      // storage/<server_id>/log/
    File snapshot_dir_; // storage/<server_id>/snapshot/

    // MemoryLog
    uint64_t start_index_;
    std::deque<Protocol::Entry> entries_;
};

} // namespace Raft

#endif // RAFT_LOG_H_
