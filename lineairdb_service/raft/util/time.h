#ifndef RAFT_TIME_H_
#define RAFT_TIME_H_

#include <chrono>
#include <cstdint>

namespace Raft {

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;
using Duration = std::chrono::milliseconds;

} // namespace Raft

#endif // RAFT_TIME_H_