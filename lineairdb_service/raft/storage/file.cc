#include "file.h"

#include <cassert>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "util/debug.h"

namespace Raft {

File::File() :
    fd_(-1),
    path_()
{}

File::File(File&& other) :
    fd_(other.fd_),
    path_(other.path_)
{
    other.fd_ = -1;
    other.path_.clear();
}

File::File(int fd, std::string path):
    fd_(fd),
    path_(path)
{}

File::~File() {
    close();
}

File& File::operator=(File&& other) {
    std::swap(fd_, other.fd_);
    std::swap(path_, other.path_);
    return *this;
}

void File::close() {
    if (fd_ < 0) return;
    if (::close(fd_) != 0) {
        FATAL("Failed to close %s: %s", path_.c_str(), strerror(errno));
    }
    fd_ = -1;
    path_.clear();
}




File openDir(const std::string& path) {
    assert(!path.empty());
    int result = mkdir(path.c_str(), 0755);
    if (result == 0) {
        syncDir(path + "/..");
    } else {
        if (errno != EEXIST) {
            FATAL("Failed to create directory %s: %s", path.c_str(), strerror(errno));
        }
    }

    int fd = open(path.c_str(), O_RDONLY|O_DIRECTORY);
    if (fd == -1) {
        FATAL("Failed to open directory %s: %s", path.c_str(), strerror(errno));
    }

    return File(fd, path);
}

File openDir(const File& dir, const std::string& child) {
    // TODO: assert?
    int result = mkdirat(dir.fd_, child.c_str(), 0755);
    if (result == 0) {
        Raft::fsync(dir);
    } else {
        if (errno != EEXIST) {
            FATAL("Failed to create directory %s: %s", child.c_str(), strerror(errno));
        }
    }

    int fd = openat(dir.fd_, child.c_str(), O_RDONLY|O_DIRECTORY);
    if (fd == -1) {
        FATAL("Failed to open directory %s: %s", child.c_str(), strerror(errno));
    }

    return File(fd, dir.path_ + "/" + child);
}

File openFile(const File& dir, const std::string& child, int flags) {
    // TODO: assert?
    int fd = openat(dir.fd_, child.c_str(), flags, 0644);   // 0644: rw-r--r--
    if (fd == -1) {
        PANIC("Failed to open %s/%s: %s", dir.path_.c_str(), child.c_str(), strerror(errno));
    }
    return File(fd, dir.path_ + "/" + child);
}

void removeFile(const File& dir, const std::string& path) {
    if (::unlinkat(dir.fd_, path.c_str(), 0) == 0) {
        return;
    }
    if (errno == ENOENT) {
        return;
    }
    PANIC("Failed to remove %s/%s: %s", dir.path_.c_str(), path.c_str(), strerror(errno));
}

/**
 * Ensures directory metadata changes (file creation/deletion) are persisted to disk by opening, syncing, and closing the directory using its path string.
 */
void syncDir(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd == -1) {
        FATAL("Failed to open %s: %s", path.c_str(), strerror(errno));
    }
    if (::fsync(fd) != 0) {
        FATAL("Failed to sync %s: %s", path.c_str(), strerror(errno));
    }
    if (close(fd) != 0) {
        WARNING("Failed to close %s: %s", path.c_str(), strerror(errno));
    }
}

/**
 * Flushes file data and metadata to disk using an already opened file descriptor from a File object reference.
 */
void fsync(const File& file) {
    if (::fsync(file.fd_) != 0) {
        FATAL("Failed to sync %s: %s", file.path_.c_str(), strerror(errno));
    }
}



} // namespace Raft