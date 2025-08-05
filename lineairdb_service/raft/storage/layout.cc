#include "layout.h"

namespace Raft {

StorageLayout::StorageLayout():
    top_dir_(),
    server_dir_()
{}

void StorageLayout::init(const std::string& ip, const std::string& port) {
    top_dir_ = openDir("storage");
    server_dir_ = openDir(top_dir_, ip + "_" + port);
}

} // namespace Raft