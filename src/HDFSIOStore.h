#ifndef _HDFSIOSTORE_H_
#define _HDFSIOSTORE_H_

#include <map>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <dirent.h>
#include <pthread.h>
#include "IOStore.h"
#include "hdfs.h"

using namespace std;

/* An implementation of the IOStore for HDFS */
class HDFSIOStore: public IOStore {
public:
    int Access(const char *path, int amode);    
    int Chmod(const char* path, mode_t mode);
    int Chown(const char *path, uid_t owner, gid_t group);
    int Close(int fd);
    int Closedir(DIR* dirp);
    int Creat(const char*path, mode_t mode);
    int Fsync(int fd);
    off_t Lseek(int fd, off_t offset, int whence);
    int Lstat(const char* path, struct stat* buf);
    int Mkdir(const char* path, mode_t mode);
    int Mknod(const char* path, mode_t mode, dev_t dev);
    void* Mmap(void *addr, size_t len, int prot, int flags, int fd, off_t offset);
    int Munmap(void *addr, size_t length);
    int Open(const char* path, int flags);
    int Open(const char* path, int flags, mode_t mode);
    DIR* Opendir(const char *name);
    ssize_t Pread(int fd, void* buf, size_t count, off_t offset);
    ssize_t Pwrite(int fd, const void* buf, size_t count, off_t offset);
    ssize_t Read(int fd, void *buf, size_t count);
    struct dirent *Readdir(DIR *dirp);
    int Rename(const char *oldpath, const char *newpath);
    int Rmdir(const char* path);
    int Stat(const char* path, struct stat* buf);
    int Symlink(const char* oldpath, const char* newpath);
    int Truncate(const char* path, off_t length);
    int Unlink(const char* path);
    int Utime(const char* filename, const struct utimbuf *times);
    ssize_t Write(int fd, const void* buf, size_t len);

    // Constructor.
    HDFSIOStore(const char* host, int port);
protected:
    hdfsFS fs; // The hdfs filesystem object for this object.
    struct openFile {
        hdfsFile file;
        int open_mode;        
    };
    map<int, struct openFile> fdMap; // Maps from int file descriptors to HDFS files.
    const char* hostName;
    int portNum;

    // What we use to manage directories.
    struct openDir {
        hdfsFileInfo* infos;
        int numEntries;
        int curEntryNum;
        struct dirent curEntry; // For returning readdir results.
    };
    
    // To assign unique fds.
    pthread_mutex_t fd_count_mutex;
    int fd_count;
    int AddFileToMap(hdfsFile file, int open_mode);
    hdfsFile GetFileFromMap(int fd);
    int GetModeFromMap(int fd);
    void RemoveFileFromMap(int fd);
private:
    // Declared but not defined, so illegal to call. We only want proper invocations of
    // our other constructor above.
    HDFSIOStore();
 
};

#endif
