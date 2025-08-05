#ifndef RAFT_UTIL_H_
#define RAFT_UTIL_H_

#include <sstream>
#include <string>
#include <vector>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace Raft {
namespace Util {

// split a string by a delimiter
inline std::vector<std::string> split(const std::string& str, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream token_stream(str);
    while (std::getline(token_stream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

// generate server id from ip and port (IP in upper 32 bits, port in lower 32 bits)
inline uint64_t generateServerId(const std::string& ip, const std::string& port) {
    struct in_addr addr;
    inet_pton(AF_INET, ip.c_str(), &addr);
    uint32_t ip_value = ntohl(addr.s_addr);
    uint64_t port_value = std::stoul(port);
    return (static_cast<uint64_t>(ip_value) << 32) | port_value;
}

// extract IP and port from serve id
inline std::string getAddressFromServerId(uint64_t server_id) {
    uint32_t ip_value = static_cast<uint32_t>(server_id >> 32);
    uint32_t port_value = static_cast<uint32_t>(server_id & 0xFFFFFFFF);

    // convert IP value to string
    struct in_addr addr;
    addr.s_addr = htonl(ip_value);
    char ip_str[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &addr, ip_str, INET_ADDRSTRLEN);

    return std::string(ip_str) + ":" + std::to_string(port_value);
}

} // namespace Util
} // namespace Raft

#endif // RAFT_UTIL_H_