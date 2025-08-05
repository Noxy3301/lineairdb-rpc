#include <iostream>
#include <gflags/gflags.h>

#include "lineairdb_server.hh"
#include "raft/raft.h"

DEFINE_string(ip, "127.0.0.1", "IP address of this server");
DEFINE_string(port, "12345", "Listen port of this server");
DEFINE_string(peers, "", "List of peers of this server (ip:port, ...)");

int main(int argc, char** argv) {
    // Parse command line arguments  
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    
    std::cout << "Starting LineairDB server..." << std::endl;
    
    // Setup Raft options
    Raft::Options raft_options;
    raft_options.ip = FLAGS_ip;
    raft_options.port = FLAGS_port;
    raft_options.peers = FLAGS_peers;
    
    LineairDBServer server;
    server.init(raft_options);  // Initialize Raft node
    server.run();               // Start listening
    return 0;
}