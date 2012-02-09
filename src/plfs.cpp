#include "plfs.h"
#include "plfs_private.h"

#include "LogicalFS.h"
#include "LogicalFD.h"
#include <assert.h>

PlfsMount *
plfs_get_mount(const char *path) 
{
    bool found = false;
    return find_mount_point(get_plfs_conf(),path,found);
}

LogicalFileSystem *
plfs_get_logical_fs(const char *path)
{
    PlfsMount *pmount = plfs_get_mount(path);
    return (pmount ? pmount->fs_ptr : NULL);
}

#define FIND_LOGICAL_FS(X) \
    LogicalFileSystem *logicalfs = plfs_get_logical_fs(X); \
    if (logicalfs == NULL) { \
        return -EINVAL; \
    } 

plfs_filetype
plfs_get_filetype(const char *path) 
{
    PlfsMount *pmount = plfs_get_mount(path);
    return (pmount ? pmount->file_type : PFT_UNKNOWN);
}

int
plfs_access(const char *path, int mask)
{
    FIND_LOGICAL_FS(path);
    return logicalfs->access(path, mask);
}

int
plfs_chmod(const char *path, mode_t mode)
{
    FIND_LOGICAL_FS(path);
    return logicalfs->chmod(path, mode);
}

int
plfs_chown(const char *path, uid_t u, gid_t g)
{
    FIND_LOGICAL_FS(path);
    return logicalfs->chown(path, u, g);
}

int
plfs_close(Plfs_fd *fd, pid_t pid, uid_t u, int open_flags,
           Plfs_close_opt *close_opt)
{
    int ret = fd->close(pid, u, open_flags, close_opt);
    if (ret <= 0) {
        delete fd;
    }
    return ret;
}

int
plfs_create(const char *path, mode_t mode, int flags, pid_t pid)
{
    FIND_LOGICAL_FS(path);
    return logicalfs->create(path, mode, flags, pid);
}

int
plfs_getattr(Plfs_fd *fd, const char *path, struct stat *st, int size_only)
{
    if (fd) {
        return fd->getattr(path, st, size_only);
    }
    FIND_LOGICAL_FS(path);
    return logicalfs->getattr(path, st, size_only);
}

int
plfs_link(const char *path, const char *to)
{
    FIND_LOGICAL_FS(to);
    return logicalfs->link(path, to);
}

int
plfs_mode(const char *path, mode_t *mode)
{
    FIND_LOGICAL_FS(path);
    return logicalfs->getmode(path, mode);
}

int
plfs_mkdir(const char *path, mode_t mode)
{
    FIND_LOGICAL_FS(path);
    return logicalfs->mkdir(path, mode);
}

int
plfs_open(Plfs_fd **pfd, const char *path, int flags, pid_t pid, mode_t m,
          Plfs_open_opt *open_opt)
{
    if (*pfd) {
        return (*pfd)->open(path, flags, pid, m, open_opt);
    }
    FIND_LOGICAL_FS(path);
    return logicalfs->open(pfd, path, flags, pid, m, open_opt);
}

int
plfs_query(Plfs_fd *fd, size_t *writers, size_t *readers,
           size_t *bytes_written, int *lazy_stat)
{
    bool reopen;
    int  ret;
    assert( fd != NULL);
    ret = fd->query(writers, readers, bytes_written, &reopen);
    if (lazy_stat) {
        PlfsConf *pconf = get_plfs_conf();
        *lazy_stat = pconf->lazy_stat && !reopen;
        mlog(MLOG_DBG, "plfs_query lazy_stat: %d.\n", *lazy_stat);
    }
    return ret;
}

ssize_t
plfs_read(Plfs_fd *fd, char *buf, size_t size, off_t offset)
{
    return fd->read(buf, size, offset);
}

int
plfs_readdir(const char *path, void *buf)
{
    FIND_LOGICAL_FS(path);
    return logicalfs->readdir(path, buf);
}

int
plfs_readlink(const char *path, char *buf, size_t bufsize)
{
    FIND_LOGICAL_FS(path);
    return logicalfs->readlink(path, buf, bufsize);
}

int
plfs_rename(const char *from, const char *to)
{
    FIND_LOGICAL_FS(from);
    return logicalfs->rename(from, to);
}

int
plfs_rmdir(const char *path)
{
    FIND_LOGICAL_FS(path);
    return logicalfs->rmdir(path);
}

int
plfs_statvfs(const char *path, struct statvfs *stbuf)
{
    FIND_LOGICAL_FS(path);
    return logicalfs->statvfs(path, stbuf);
}

int
plfs_symlink(const char *path, const char *to)
{
    FIND_LOGICAL_FS(to);
    return logicalfs->symlink(path, to);
}

int
plfs_sync(Plfs_fd *fd, pid_t pid)
{
    return fd->sync(pid);
}

int
plfs_trunc(Plfs_fd *fd, const char *path, off_t offset, int open_file)
{
    if (fd) {
        return fd->trunc(path, offset);
    }
    FIND_LOGICAL_FS(path);
    return logicalfs->trunc(path, offset, open_file);
}

int
plfs_unlink(const char *path)
{
    FIND_LOGICAL_FS(path);
    return logicalfs->unlink(path);
}

int
plfs_utime(const char *path, struct utimbuf *ut)
{
    FIND_LOGICAL_FS(path);
    return logicalfs->utime(path, ut);
}

ssize_t
plfs_write(Plfs_fd *fd, const char *buf, size_t count,
           off_t offset, pid_t pid)
{
    return fd->write(buf, count, offset, pid);
}

// Should these functions be exposed to FUSE or ADIO?
int
plfs_flatten_index(Plfs_fd *fd, const char *logical)
{
    return fd->compress_metadata(logical);
}
