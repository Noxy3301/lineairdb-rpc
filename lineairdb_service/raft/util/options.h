#ifndef RAFT_OPTIONS_H_
#define RAFT_OPTIONS_H_

#include <string>

namespace Raft {

struct Options {
    std::string ip;
    std::string port;
    std::string peers;
};

} // namespace Raft

#endif // RAFT_OPTIONS_H_