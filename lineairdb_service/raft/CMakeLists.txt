find_package(Protobuf REQUIRED)

set(PROTO_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/consensus/raft.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/storage/metadata.proto
)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

add_library(raft_core OBJECT
    ${PROTO_SRCS}
    ${PROTO_HDRS}
    consensus/consensus.cc
    rpc/rpc_server.cc
    peer/socket.cc
    peer/peer.cc
    raft.cc
    storage/file.cc
    storage/log.cc
    storage/layout.cc
)

target_include_directories(raft_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/rpc
    ${CMAKE_CURRENT_SOURCE_DIR}/socket
    ${CMAKE_CURRENT_BINARY_DIR}
    ${PROTOBUF_INCLUDE_DIRS}
)

target_link_libraries(raft_core PUBLIC
    pthread
    ${PROTOBUF_LIBRARIES}
)