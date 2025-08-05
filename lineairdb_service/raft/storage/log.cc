#include <fcntl.h>

#include <google/protobuf/text_format.h>
#include <unistd.h>

#include "log.h"
#include "util/debug.h"

namespace Raft {

Log::Log(const File& parent_dir) :
    log_dir_(openDir(parent_dir, "log")),
    snapshot_dir_(openDir(parent_dir, "snapshot")),
    start_index_(1),
    entries_()
{}

File protoToFile(const google::protobuf::Message& in, const File& dir, const std::string& path) {
    File file = openFile(dir, path, O_CREAT|O_WRONLY|O_TRUNC);
    std::string contents;
    google::protobuf::TextFormat::PrintToString(in, &contents);
    contents = "\n" + contents;

    // TODO: checksum?

    // TODO: should use writev instead of write
    size_t total_written = 0;
    size_t length = contents.size();
    const char* data = contents.c_str();
    while (total_written < length) {
        ssize_t written = write(file.fd_, data + total_written, length - total_written);
        if (written == -1) {
            if (errno == EINTR) {
                continue; // interrupted by signal, retry
            }
            PANIC("Failed to write to %s: %s", path.c_str(), strerror(errno));
        }
        total_written += written;
    }

    if (::fsync(file.fd_) == -1) {
        PANIC("Failed to fsync %s: %s", path.c_str(), strerror(errno));
    }

    return file;
}

std::string getLogFileName(uint64_t index) {
    std::stringstream ss;
    ss << std::setw(16) << std::setfill('0') << std::hex << index;
    return ss.str();
}

// MEMO: LogCabin uses sync queue for async disk I/O, but we use direct fsync for simplicity.
std::pair<uint64_t, uint64_t> Log::append(const std::vector<const Protocol::Entry*>& new_entries) {
    // MemoryLog::append
    uint64_t first_index = start_index_ + entries_.size();
    uint64_t last_index = first_index + new_entries.size() - 1;
    for (auto it = new_entries.begin(); it != new_entries.end(); ++it) {
        entries_.push_back(**it);
    }
    
    for (uint64_t index = first_index; index <= last_index; ++index) {
        File file = protoToFile(getEntry(index), log_dir_, getLogFileName(index));
        ::fsync(file.fd_);
        file.close();
    }
    File mdfile = updateMetadataCallerSync();
    ::fsync(log_dir_.fd_);
    ::fsync(mdfile.fd_);
    mdfile.close();
    // TODO: should we store last_index in Log?
    // current_sync_->last_index = last_index;

    return {first_index, last_index};
}

const Protocol::Entry& Log::getEntry(uint64_t index) const {
    uint64_t offset = index - start_index_;
    return entries_.at(offset);
}

uint64_t Log::getLogStartIndex() const {
    return start_index_;
}

uint64_t Log::getLastLogIndex() const {
    return start_index_ + entries_.size() - 1;
}

uint64_t Log::getSizeBytes() const {
    uint64_t size = 0;
    for (auto &entry : entries_) {
        size += entry.ByteSize();
    }
    return size;
}

void Log::truncatePrefix(uint64_t first_index) {
    uint64_t old = getLogStartIndex();
    // MemoryLog::truncatePrefix
    if (first_index > start_index_) {
        // erase entries in range [start_index, first_index), so deque offsets in range [0, first_index - start_index)
        entries_.erase(entries_.begin(), entries_.begin() + static_cast<int64_t>(std::min(first_index - start_index_, entries_.size())));
        start_index_ = first_index;
    }

    updateMetadata();
    for (uint64_t entry_id = old; entry_id < getLogStartIndex(); ++entry_id) {
        removeFile(log_dir_, getLogFileName(entry_id));
    }
}

void Log::truncateSuffix(uint64_t last_index) {
    uint64_t old = getLastLogIndex();
    // MemoryLog::truncateSuffix
    if (last_index < start_index_) {
        entries_.clear();
    } else if (last_index < start_index_ - 1 + entries_.size()) {
        entries_.resize(last_index - start_index_ + 1);
    }

    updateMetadata();
    for (uint64_t entry_id = old; entry_id > getLastLogIndex(); --entry_id) {
        removeFile(log_dir_, getLogFileName(entry_id));
    }
}

void Log::updateMetadata() {
    File mdfile = updateMetadataCallerSync();
    ::fsync(mdfile.fd_);
    ::fsync(log_dir_.fd_);
    mdfile.close();
}

File Log::updateMetadataCallerSync() {
    *log_metadata_.mutable_raft_metadata() = raft_metadata_;
    log_metadata_.set_entries_start(getLogStartIndex());
    log_metadata_.set_entries_end(getLastLogIndex());
    log_metadata_.set_version(log_metadata_.version() + 1);
    if (log_metadata_.version() % 2 == 1) {
        return protoToFile(log_metadata_, log_dir_, "metadata1");
    } else {
        return protoToFile(log_metadata_, log_dir_, "metadata2");
    }
}


} // namespace Raft