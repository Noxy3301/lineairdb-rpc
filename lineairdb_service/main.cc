#include <iostream>
#include <gflags/gflags.h>

#include "lineairdb_server.hh"
#include "raft/raft.h"

DEFINE_string(ip, "127.0.0.1", "IP address of this server");
DEFINE_string(port, "12345", "Listen port of this server");
DEFINE_string(peers, "", "List of peers of this server (ip:port, ...)");

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    
    std::cout << "Starting LineairDB server..." << std::endl;
    
    // Setup LineairDB config (TODO: make configurable)
    LineairDB::Config lineairdb_config;
    lineairdb_config.enable_checkpointing = false;
    lineairdb_config.enable_recovery = false;
    lineairdb_config.max_thread = 1;
    
    // Setup Raft options
    Raft::Options raft_options;
    raft_options.ip = FLAGS_ip;
    raft_options.port = FLAGS_port;
    raft_options.peers = FLAGS_peers;
    
    LineairDBServer server;
    server.init_lineairdb(lineairdb_config);
    server.init_raft(raft_options);
    server.run();
    return 0;
}