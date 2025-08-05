#ifndef RAFT_LAYOUT_H_
#define RAFT_LAYOUT_H_

#include "file.h"

namespace Raft {

class StorageLayout {
public:
    StorageLayout();
    ~StorageLayout() = default; // TODO: add destructor

    void init(const std::string& ip, const std::string& port);

    File top_dir_;      // storage/
    File server_dir_;   // storage/<server_id>/

};

} // namespace Raft


#endif // RAFT_LAYOUT_H_