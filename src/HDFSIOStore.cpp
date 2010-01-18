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

}
