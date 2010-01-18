#include <errno.h>
#include <sys/types.h>
#include <grp.h>
#include <pwd.h>
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
    pthread_mutex_init(&fd_count_mutex);
}

/**
 * Adds the specified file to the fdMap, usually on open.
 * it is removed on close.
 * Returns the new fd on success, -1 on error.
 */
int HDFSIOStore::AddFileToMap(hdfsFile file)
{
    int fd;
    pthread_mutex_lock(&fd_count_mutex);
    fd = fd_count++;
    pthread_mutex_unlock(&fd_count_mutex);
    fdMap[fd] = file;

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
    map<int, hdfsFile>::iterator it;
    it= fdMap.find(fd);
    if (it == fdMap.end()) {
        return NULL;
    }
    return it->second;
}

/**
 * Access. For now we assume that we're the owner of the file!
 * This should be changed. So the only thing we really check is 
 * existence.
 */
int HDFSIOStore::Access(const char* path, int mode) 
{
    // hdfsExists says it returns 0 on success, but I believe this to be wrong documentation.
    if ((mode & F_OK) && !hdfsExists(fs, path)) {
        errno = ENOENT;
        return -1;
    }
    return 0;
}

int HDFSIOStore::Chmod(const char* path, mode_t mode) 
{
    return hdfsChmod(fs, path, mode);
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
int HDFSIOSTORE::Close(int fd) 
{
    int ret;
    hdfsFile openFile;
    openFile = GetFileFromMap(fd);
    if (!openFile) {
        errno = ENOENT;
        return -1;
    }
    ret = hdfsCloseFile(fs, openFile);
    if (!ret)
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
    hdfsFile file = hdfsOpenFile(fs, path, O_WRONLY, 0, 0, 0);
    if (!file)
        return -1;
    hdfsChmod(fs, path, mode);
    
    // We've created the file, set its mode. Now add it to map!
    fd = AddFileToMap(file);
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
 */
int HDFSIOStore::Lseek(int fd, off_t offset, int whence)
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
 * Lstat. This one is mostly a matter of datat structure conversion to keep everything
 * right.
 */
int HDFSIOStore::Lstat(const char* path, struct stat* buf)
{
    hdfsFileInfo* hdfsInfo = hdfsGetPathInfo(fs, path);
    if (!hdfsInfo) {
        errno = ENOENT;
        return -1;
    }
    // Maybe the Hadoop users are the names of an actual user of the system!
    // If so, use that. Otherwise, we'll use 0 for both, which should correspond
    // to root.
    struct group* group_s = getgrnam(hdfsInfo->mGroup);
    struct passwd* passwd_s = getpwnam(hdfsInfo->mOwner); 

    buf->st_dev = 0;
    buf->st_ino = 0; // I believe this is ignored by FUSE.

    // We need the mode to be both the permissions and some additional info,
    // based on whether it's a directory of a file.
    buf->mode = hdfsInfo->mPermissions;
    if (hdfsInfo->mKind == kObjectKindFile) {
        buf->mode |= S_IFREG;
    } else {
        buf->mode |= S_IFDIR;
    }
        
    if (passwd_s)
        buf->st_uid = passwd_s->pw_uid;
    else
        buf->st_uid = 0;
    if (group_s)
        buf->st_gid = group_s->gr_gid;
    else
        buf->st_gid = 0;
    buf->st_rdev = 0; // I'm not sure about this one!
    buf->st_size = hdfsInfo->mSize;
    buf->st_blksize = hdfsInfo->mBlockSize;
    // This is supposed to indicate the actual number of sectors allocated
    // to the file, and is used to calculate storage capacity consumed per
    // File. Files might have holes, but on HDFS we lie about this anyway.
    // So I'm just returning the size/512. Note that if this is supposed to
    // represent storage consumed, it's actually 3 times this due to replication.
    buf->st_blocks = hdfsInfo->mSize/512; 
    buf->st_atime = hdfs->mLastAccess;
    buf->st_mtime = hdfs->mLastMod;
    // This one's a total lie. There's no tracking of metadata change time
    // in HDFS, so we'll just use the modification time again.
    buf->st_ctime = hdfs->mLastMod;

    return 0;
}

/**
 * A bit easier than Creat since directory creation is pretty straightforward.
 */
int HDFSIOStore::Mkdir(const char* path, mode_t mode)
{
    if (hdfsCreateDirectory(fs, path)) {
        return -1;
    }

    return hdfsChmod(fs, path, mode));
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
    int bytes_read=0;
    char* buffer;
    hdfsFile openFile = GetFileFromMap(fd);
    
    if (!openFile) {
        errno = EBADF;
        return NULL;
    }

    // We ignore most of the values passed in.
    // Allocate enough space for the lenth.
    char* buffer = new char[len];

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
 * Open is only tricky because HDFS does not support certain modes of operation.
 * For example, we read or write--not both.
 * It really only supports O_RDONLY and O_WRONLY and O_WRONLY|APPEND.
 */
int HDFSIOStore::Open(const char* path, int flags)
{
    // TODO: More thought into what the flags should do.
    int new_flags;
    hdfsFile openFile;
    int fd;

    if (flags & O_RDONLY) {
        new_flags = O_RDONLY;
    } else if (flags & O_WRONLY) {
        new_flags = O_WRONLY;
    } else {
        errno = ENOTSUP;
        return -1;
    }
    openFile = hdfsOpenFile(fs, path, new_flags, 0, 0, 0);
    
    if (!openFile) {
        return -1;
    }

    fd = AddFileToMap(openFile);
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
 */
DIR* HDFSIOStore::Opendir(const char *name) 
{
    openDir* dir = new openDir;
    // I assume new returns NULL on error.
    if (!dir)
        return dir;
    dir->curEntry = 0;
    // We have space to remember the directory. Now slurp in all its files.
    dir->infos = hdfsListDirectory(fs, name, &dir->numEntries);
    if (!dir->infos) {
        delete dir;
        return NULL;
    }

    return (DIR*)dir;
}

ssize_t HDFSIOStore::Pread(int fd, void* buf, size_t count, off_t offset)
{
    hdfsFile openFile = GetFileFromMap(fd);
    if (!openFile) {
        errno = EBADF;
        return -1;
    }
    return hdfsPread(fs, openFile, offset, buf, count);
}
