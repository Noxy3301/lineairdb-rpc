#ifndef RAFT_FILE_H_
#define RAFT_FILE_H_

#include <string>

namespace Raft {

class File {
public:
    File();
    File(int fd, std::string path);
    ~File();

    // move and assignment
    File(File&& other);
    File& operator=(File&& other);

    // disable copy
    File(const File&) = delete;
    File& operator=(const File&) = delete;

    void close();
    int release();

    int fd_;
    std::string path_;
};


File openDir(const std::string& path);
File openDir(const File& dir, const std::string& child);

File openFile(const File& dir, const std::string& child, int flags);

void removeFile(const File& dir, const std::string& path);

void syncDir(const std::string& path);
void fsync(const File& file);


} // namespace Raft


#endif // RAFT_FILE_H_