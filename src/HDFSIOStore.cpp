#include <errno.h>
#include <iostream>
#include <sys/types.h>
#include <grp.h>
#include <pwd.h>
#include <string.h>
#include "Util.h"
#include "HDFSIOStore.h"


/**
 * Constructor. Initializes some objects and attempts to connect to the
 * HDFS Filesystem.
 * We start the fd count at a value which is without special meaning.
 */
HDFSIOStore::HDFSIOStore(const char* host, int port)
    : fdMap(), hostName(host), portNum(port), fd_count(3)
{
    // Get ourselves a filesystem connection.
    fs = hdfsConnect(hostName, portNum);

    // Initialize our mutex for protecting the fd count.
    pthread_mutex_init(&fd_count_mutex, NULL);
}

/**
 * Adds the specified file to the fdMap, usually on open.
 * it is removed on close.
 * Returns the new fd on success, -1 on error.
 */
int HDFSIOStore::AddFileToMap(hdfsFile file, int open_mode)
{
    int fd;
    struct openFile of;
    of.file = file;
    of.open_mode = open_mode;
    pthread_mutex_lock(&fd_count_mutex);
    fd = fd_count++;
    pthread_mutex_unlock(&fd_count_mutex);
    fdMap[fd] = of;

    return fd;
}

/**
 * Removes the specified fd from the Map.
 */
void HDFSIOStore::RemoveFileFromMap(int fd)
{
    fdMap.erase(fd);
}

/** 
 * Translates a integral fd to the opaque hdfsFile object.
 * Returns null if it's not present.
 */
hdfsFile HDFSIOStore::GetFileFromMap(int fd)
{
    map<int, openFile>::iterator it;
    it= fdMap.find(fd);
    if (it == fdMap.end()) {
        return NULL;
    }
    return it->second.file;
}

int HDFSIOStore::GetModeFromMap(int fd)
{
    map<int, openFile>::iterator it;
    it= fdMap.find(fd);
    if (it == fdMap.end()) {
        return NULL;
    }
    return it->second.open_mode;
}

/**
 * Access. For now we assume that we're the owner of the file!
 * This should be changed. So the only thing we really check is 
 * existence.
 */
int HDFSIOStore::Access(const char* path, int mode) 
{
    // hdfsExists says it returns 0 on success, but I believe this to be wrong documentation.
    if ((mode & F_OK) && hdfsExists(fs, path)) {
        errno = ENOENT;
        return -1;
    }
    return 0;
}

int HDFSIOStore::Chmod(const char* path, mode_t mode) 
{
    return 0; //hdfsChmod(fs, path, mode);
}

/**
 * Chown. HDFS likes strings, not constants. So we translate these.
 * And hope that the same name exists in the Hadoop DB
 */
int HDFSIOStore::Chown(const char *path, uid_t owner, gid_t group) 
{
    struct group* group_s = getgrgid(group);
    struct passwd* passwd_s = getpwuid(owner);

    if (!group_s || !passwd_s)
        return -1;

    return hdfsChown(fs, path, passwd_s->pw_name, group_s->gr_name);
}

/**
 * Close. Simple: Look up the hdfsFile in the map, close it if it exists,
 * and remove it from the map.
 */
int HDFSIOStore::Close(int fd) 
{
    int ret;
    hdfsFile openFile;

    openFile = GetFileFromMap(fd);
    //std::cout << "Closing " << fd << "\n";
    if (!openFile) {
        //std::cout << "Error looking it up in map.\n";
        errno = ENOENT;
        return -1;
    }
    /*    std::cout << "FD " << fd << " corresponds to hdfsFile " << openFile << "\n";*/
    //if (GetModeFromMap(fd) == O_WRONLY) {
    if (hdfsFlush(fs, openFile)) {
        Util::Debug("Couldn't flush file. Probably open for reads.\n");
    }
    //}
    ret = hdfsCloseFile(fs, openFile);
    if (ret) {
        //std::cout << "Error closing hdfsFile\n";
        return ret;
    }

    /*    // DEBUGGING: Try to re-open the file to debug close error.
    open_path = GetPathFromMap(fd);
    std::cout << "Re-opening " << open_path << "\n";
    openFile = hdfsOpenFile(fs, open_path->c_str(), O_RDONLY,0,0,0);
    if (!openFile) {
        std::cout << "Error re-opening the file!" << errno << "\n";
    } else {
        std::cout << "Could successfully re-open. Interesting....\n";
        hdfsCloseFile(fs, openFile);
    }
    */
    RemoveFileFromMap(fd);
    return ret;
}

/**
 * On open, we create a struct openDir. Here, on close, we delete the fields and
 * deallocate the structure.
 */
int HDFSIOStore::Closedir(DIR* dirp)
{
    struct openDir *theDir = (struct openDir*)dirp;
    hdfsFreeFileInfo(theDir->infos, theDir->numEntries);
    delete theDir;
    return 0;
}

/**
 * Creat. Just create the file by opening it. Then set the mode. Finally, add it to our map and
 * return the fd.
 * There's a slight problem here because HDFS doesn't support an atomic open/create. :-(
 * Fortunately, creat does imply truncate, so I don't think it'll be a problem.
 */
int HDFSIOStore::Creat(const char*path, mode_t mode)
{
    int fd;
    string path_string(path);
    hdfsFile file = hdfsOpenFile(fs, path, O_WRONLY, 0, 0, 0);
    if (!file)
        return -1;
    //hdfsChmod(fs, path, mode);
    
    // We've created the file, set its mode. Now add it to map!
    fd = AddFileToMap(file, O_WRONLY);
    return fd;
}

int HDFSIOStore::Fsync(int fd)
{
    hdfsFile openFile = GetFileFromMap(fd);
    if (!openFile) {
        errno = EBADF;
        return -1;
    }
    return hdfsFlush(fs, openFile);
}

/**
 * Lseek. Ugh. The tricky part here is the whence parameter.
 * HDFS provides with a seek that only works for an absolute byte offset.
 * The TRICKIER part is this will fail on files open for writes.
 * There's no way around this limitation in HDFS, but fortunately in 
 * PLFS we never update inplace, but always write to the end. So hopefully
 * it won't arise.
 */
off_t HDFSIOStore::Lseek(int fd, off_t offset, int whence)
{
    tOffset new_offset;
    hdfsFile openFile = GetFileFromMap(fd);
    if (!openFile) {
        errno = EBADF;
        return -1;
    }
    switch (whence)  {
    case SEEK_SET:
        new_offset = offset;
        break;
    case SEEK_CUR:
        new_offset = hdfsTell(fs, openFile) + offset;
        break;
    case SEEK_END:
        // I'm not entirely sure about this call. I'm assuming hdfsAvailable()
        // returns the total number of bytes remaining in the file. If it's
        // less, then this calculation will be wrong. We can definitely get
        // the size through a hdfsGetPathInfo() call. However, this requires
        // a path, not an hdfsFile. This would require storing paths in our
        // map as well. At this point, I'm reluctant to do that.
        new_offset = hdfsTell(fs, openFile) + hdfsAvailable(fs, openFile) + offset;
        break;
    }

    if (hdfsSeek(fs, openFile, new_offset)) {
        return -1;
    } 
    return new_offset;
}

/**
 * Lstat. With no symbolic links in HDFS, this is just a call to Stat().
 */
int HDFSIOStore::Lstat(const char* path, struct stat* buf)
{
    return Stat(path, buf);
}

/**
 * A bit easier than Creat since directory creation is pretty straightforward.
 */
int HDFSIOStore::Mkdir(const char* path, mode_t mode)
{
    if (hdfsCreateDirectory(fs, path)) {
        return -1;
    }

    return hdfsChmod(fs, path, mode);
}

/**
 * We just create a regular file with this, ignoring dev.
 * So it's just a call to Creat.
 */
int HDFSIOStore::Mknod(const char* path, mode_t mode, dev_t dev) 
{
    return Creat(path, mode);
}

/**
 * MMap is fairly awkward and will go away in favor of a call to replace it
 * for the one task we use it for: Efficiently reading in the index file.
 */
void* HDFSIOStore::Mmap(void *addr, size_t len, int prot, int flags, int fd, off_t offset) 
{
    size_t bytes_read=0;
    char* buffer;
    hdfsFile openFile = GetFileFromMap(fd);
    
    if (!openFile) {
        errno = EBADF;
        return NULL;
    }

    // We ignore most of the values passed in.
    // Allocate enough space for the lenth.
    buffer = new char[len];

    if (!buffer) {
        return NULL;
    }
    // Try to read in the file.
    while (bytes_read < len) {
        int res = hdfsPread(fs, openFile, offset+bytes_read, 
                            buffer+bytes_read, len-bytes_read);
        if (res == -1) {
            // An error in reading.
            delete[] buffer;
            return NULL;
        }
        bytes_read += res;
    }

    return buffer;
}

/**
 * Just clean up the allocated space. No real error checking here.
 */
int HDFSIOStore::Munmap(void* addr, size_t len)
{
    char* buffer = (char*)addr;
    delete[] buffer;
    return 0;
}

/**
 * Open is only tricky because HDFS does not support certain modes of operation.
 * For example, we read or write--not both.
 * It really only supports O_RDONLY and O_WRONLY and O_WRONLY|APPEND.
 * (And the last one not really)
 */
int HDFSIOStore::Open(const char* path, int flags)
{
    // TODO: More thought into what the flags should do.
    int new_flags;
    hdfsFile openFile;
    int fd;
    string path_string(path);
    
    if (flags == O_RDONLY) {
        new_flags = O_RDONLY;
    } else if (flags & O_WRONLY)  {
        new_flags = O_WRONLY;
    } else if (flags & O_RDWR) {
        if (!hdfsExists(fs, path)) {
            // If the file exists, open Read Only!
            flags = O_RDONLY;
        } else {
            flags = O_WRONLY;
        }
    } else {
        //std::cout << "Unsupported flags.\n";
        errno = ENOTSUP;
        return -1;
    }

    //std::cout << "Attempting open on " << path << "\n";
    /*if (hdfsExists(fs, path)) {
        std::cout << "Doesn't exist yet....\n";
    } else {
        hdfsFileInfo* info = hdfsGetPathInfo(fs, path);
        std::cout << "Exists and has " << info->mSize << " bytes\n";
        hdfsFreeFileInfo(info, 1);
    }*/
    openFile = hdfsOpenFile(fs, path, new_flags, 0, 0, 0);
    
    if (!openFile) {
        //std::cout << "Disaster trying to open " << path << "\n";
        return -1;
    }
    fd = AddFileToMap(openFile, new_flags);

    //std::cout << "Opened fd " << fd << "with secret file " << openFile << "\n";
    return fd;
}

/**
 * As the previous open, but with a chmod as well.
 */
int HDFSIOStore::Open(const char* path, int flags, mode_t mode)
{
    int fd = Open(path, flags);
    Chmod(path, mode);

    return fd;
}

/**
 * Opendir. A bit of abuse here... The DIR* functions don't 
 * return a pointer to a directory stream. Instead they return a
 * pointer to a struct openDir, which we use to satisfy future
 * requests.
 * NOTE:
 * There's a slight issue I have with this code--the allocation is
 * in the memory space of the FUSE client, not the application. If the
 * application exits before calling Closedir(), it's a memory leak.
 * I'm not sure what the appropriate technique around this is; even if
 * we reslurp in the entire directory on every call to readdir so we
 * don't need to keep around the array of all file infos, we still need
 * some place to store our counter.
 * One way around this is to replace the entire readdir logic of Util
 * and Container with functionality that stores the contents of the
 * directory in some buffer, and let the higher level manage it. Not
 * sure how that would work with readdir fuse calls, however.
 */
DIR* HDFSIOStore::Opendir(const char *name) 
{
    openDir* dir = new openDir;
    // I assume new returns NULL on error.
    if (!dir)
        return NULL;
    dir->curEntryNum = 0;
    // We have space to remember the directory. Now slurp in all its files.
    dir->infos = hdfsListDirectory(fs, name, &dir->numEntries);
    if (!dir->infos) {
        delete dir;
        return NULL;
    }
    
    // Temporary debugging measure:
    for (int i = 0; i < dir->numEntries; i++) {
        Util::Debug("%s\n", dir->infos[i].mName);
    }

    return (DIR*)dir;
}

/**
 * Pread. A simple wrapper around the HDFS call.
 */
ssize_t HDFSIOStore::Pread(int fd, void* buf, size_t count, off_t offset)
{
    hdfsFile openFile = GetFileFromMap(fd);
    if (!openFile) {
        errno = EBADF;
        return -1;
    }
    //std::cout << "Reading " << count << "bytes\n";
    return hdfsPread(fs, openFile, offset, buf, count);
}

/**
 * Pwrite. There is no pwrite in HDFS, since we can only write to the end of a
 * file. So this will always fail.
 */
ssize_t HDFSIOStore::Pwrite(int fd, const void* buf, size_t count, off_t offset) 
{
    errno = ENOTSUP;
    return -1;
}

/**
 * Read. A simple wrapper around the HDFS call.
 */
ssize_t HDFSIOStore::Read(int fd, void *buf, size_t count)
{
    hdfsFile openFile = GetFileFromMap(fd);
    if (!openFile) {
        errno = EBADF;
        return -1;
    }
    //std::cout << "Reading " << count << "bytes\n";
    return hdfsRead(fs, openFile, buf, count);
}

/**
 * Readdir. Remember that the dirp is actually struct openDir*. We cast it
 * and then fill in the struct dirent curEntry embedded in it, using the
 * hdfsFileInfo* infos array and the curEntryNum count.  
 * Then we return the address of that embedded struct dirent.
 * One annoying feature: The mName field of file info is the FULL PATH. So
 * we have to strip it.
 */
struct dirent *HDFSIOStore::Readdir(DIR *dirp)
{
    Util::Debug("readdir called\n");
    char* lastComponent; // For locating the last part of the string name.
    struct openDir* dir = (struct openDir*)dirp;
    if (dir->curEntryNum == dir->numEntries) {
        // We've read all the entries! Return NULL.
        Util::Debug("Done reading directory\n");
        return NULL;
    }
    //std::cout << "Processing entry.\n";
    // Fill in the struct dirent curEntry field.
    dir->curEntry.d_ino = 0; // No inodes in HDFS.
    // I'm not sure how offset is used in this context. Technically
    // only d_name is required in non-Linux deployments.
    dir->curEntry.d_off = 0;
    dir->curEntry.d_reclen = sizeof(struct dirent);
    dir->curEntry.d_type = (dir->infos[dir->curEntryNum].mKind == kObjectKindFile
                            ? DT_REG : DT_DIR);

    lastComponent = strrchr(dir->infos[dir->curEntryNum].mName, '/');
    if (!lastComponent) { 
        // No slash in the path. Just use the whole thing.
        lastComponent = dir->infos[dir->curEntryNum].mName;
    } else {
        // We want everything AFTER the slash.
        lastComponent++;
    }
    strncpy(dir->curEntry.d_name, lastComponent, NAME_MAX);
    // NAME_MAX does not include the terminating null and if we copy
    // the max number of characters, strncpy won't place it, so set it manually.
    dir->curEntry.d_name[NAME_MAX] = '\0';
    //std::cout << "Entry name " << dir->curEntry.d_name[NAME_MAX];
    dir->curEntryNum++;
    return &dir->curEntry;
}

/**
 * Rename. A wrapper around the hdfs rename call, really.
 */
int HDFSIOStore::Rename(const char *oldpath, const char *newpath)
{
    return hdfsRename(fs, oldpath, newpath);
}

/**
 * Rmdir. HDFS doesn't distinguish between deleting a file and a directory.
 */
int HDFSIOStore::Rmdir(const char* path)
{
    return hdfsDelete(fs, path);
}
/**
 * Stat. This one is mostly a matter of datat structure conversion to keep everything
 * right.
 */
int HDFSIOStore::Stat(const char* path, struct stat* buf)
{
    hdfsFileInfo* hdfsInfo = hdfsGetPathInfo(fs, path);
    if (!hdfsInfo) {
        //std::cout << "Failed to get stat for path " << path << "\n";
        errno = ENOENT;
        return -1;
    }
    // Maybe the Hadoop users are the names of an actual user of the system!
    // If so, use that. Otherwise, we'll use 0 for both, which should correspond
    // to root.
    struct group* group_s = getgrnam(hdfsInfo->mGroup);
    struct passwd* passwd_s = getpwnam(hdfsInfo->mOwner); 

    buf->st_dev = 0; // Unused by FUSE, I believe.
    buf->st_ino = 0; // There are no inode numbers in HDFS.

    // We need the mode to be both the permissions and some additional info,
    // based on whether it's a directory of a file.
    buf->st_mode = hdfsInfo->mPermissions;
    if (hdfsInfo->mKind == kObjectKindFile) {
        buf->st_mode |= S_IFREG;
    } else {
        buf->st_mode |= S_IFDIR;
    }
        
    if (passwd_s)
        buf->st_uid = passwd_s->pw_uid;
    else
        buf->st_uid = 0;
    if (group_s)
        buf->st_gid = group_s->gr_gid;
    else
        buf->st_gid = 0;
    buf->st_rdev = 0; // Hopefully unused.
    buf->st_size = hdfsInfo->mSize;
    buf->st_blksize = hdfsInfo->mBlockSize;
    // This is supposed to indicate the actual number of sectors allocated
    // to the file, and is used to calculate storage capacity consumed per
    // File. Files might have holes, but on HDFS we lie about this anyway.
    // So I'm just returning the size/512. Note that if this is supposed to
    // represent storage consumed, it's actually 3 times this due to replication.
    buf->st_blocks = hdfsInfo->mSize/512; 
    buf->st_atime = hdfsInfo->mLastAccess;
    buf->st_mtime = hdfsInfo->mLastMod;
    // This one's a total lie. There's no tracking of metadata change time
    // in HDFS, so we'll just use the modification time again.
    buf->st_ctime = hdfsInfo->mLastMod;
    hdfsFreeFileInfo(hdfsInfo, 1);
    return 0;
}

/**
 * Statvfs. This one we actually could fill in, but I have not yet done so, as PLFS's
 * logic doesn't depend on it.
 */
int HFDFSIOStore::Statvfs( const char *path, struct statvfs* stbuf )
{
	errno = EIO; // This seems the most appropriate, although EACCES is arguable.
	return -1;
}


/** 
 * Symlink. HDFS does not support hard or soft links. So we set errno and return.
 */
int HDFSIOStore::Symlink(const char* oldpath, const char* newpath)
{
    errno = EPERM; // According to POSIX spec, this is the proper code.
    return -1;
}

/**
 * We don't support soft links on HDFS.
 */
ssize_t HDFSIOStore::Readlink(const char*link, char *buf, size_t bufsize)
{
	errno = EINVAL;
	return -1;
}


/**
 * Truncate. In HDFS, we can truncate a file to 0 by re-opening it for O_WRONLY.
 * I'm not sure how this will behave if it's attempted on a file already open.
 * Truncating to anything larger than 0 but smaller than the filesize is impossible.
 * Increasing the filesize can be accomplished by writing 0's to the end of the file,
 * but this requires HDFS to support append, which has always been on and off again
 * functionality in HDFS.
 * For these reasons, we're only supporting it for lengths of 0.
 */
int HDFSIOStore::Truncate(const char* path, off_t length)
{
    hdfsFile truncFile;
    if (length > 0) {
        errno = EINVAL; // EFBIG is also an option.
        return -1;
    }
    // Make sure the file exists. A truncate call should not CREATE a new
    // file.
    // BUG: This should really be made atomic with the open call. Otherwise
    // the following sequence is possible:
    // 1. Thread A calls truncate() on 'foo'. It does the hdfsExists() check
    // below and determines that the file exists.
    // 2. Thread B begins running and deletes 'foo'.
    // 3. Thread A begins running again and performs the truncating open call.
    // This creates a new zero-length file.
    // This situation can be avoided with a mutex controlling delete and truncate,
    // but for the time being, I think this is too expensive for the behavior
    // benefits.
    if (hdfsExists(fs, path)) { // -1 if it doesn't exist.
        errno = ENOENT;
        return -1;
    }
    // Attempt to open the file to truncate it.
    truncFile = hdfsOpenFile(fs, path, O_WRONLY, 0, 0, 0);
    if (!truncFile) {
        return -1;
    }

    hdfsCloseFile(fs, truncFile);
    return 0;
}

/**
 * Unlink. HDFS lacks the concept of links, so this is really just a delete.
 */
int HDFSIOStore::Unlink(const char* path)
{
    return hdfsDelete(fs, path);
}

/**
 * Utime. More translation mostly.
 */
int HDFSIOStore::Utime(const char* filename, const struct utimbuf *times)
{
    Util::Debug("Running utime\n");
    return hdfsUtime(fs, filename, times->modtime, times->actime);
}

/**
 * Write. Similar to read; just lookup the file and issue the write.
 */
ssize_t HDFSIOStore::Write(int fd, const void* buf, size_t len)
{
    hdfsFile openFile = GetFileFromMap(fd);
    if (!openFile) {
        errno = EBADF;
        return -1;
    }
    return hdfsWrite(fs, openFile, buf, len);
}
