#ifndef RAFT_MESSAGE_HEADER_H
#define RAFT_MESSAGE_HEADER_H

#include <cstdint>

namespace Raft {

struct MessageHeader {
    uint64_t sender_id;
    uint32_t message_type;
    uint32_t payload_size;
} __attribute__((packed));


enum MessageType {
    REQUEST_VOTE_REQUEST = 1,
    REQUEST_VOTE_RESPONSE = 2,
    APPEND_ENTRIES_REQUEST = 3,
    APPEND_ENTRIES_RESPONSE = 4,
};

} // namespace Raft

#endif