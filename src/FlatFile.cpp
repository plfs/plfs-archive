#include "FlatFile.h"
#include "Util.h"
#include "plfs_private.h"
#include "OpenFile.h"
#include "Metadata.h"

#define FLAT_ENTER PLFS_ENTER

#define FLAT_EXIT(X) if (X<0) X = -errno; PLFS_EXIT(X);

#define FLAT_EXPAND_TO \
    string old_canonical = path; \
    string new_canonical; \
    ExpansionInfo exp_info; \
    new_canonical = expandPath(to,&exp_info,EXPAND_CANONICAL,-1,0);

int 
flatfile_create( const char *logical, mode_t mode, int flags, pid_t pid ){
    FLAT_ENTER;
    ret = Util::Creat(path.c_str(),mode);
    FLAT_EXIT(ret);
}

int 
flatfile_chown( const char *logical, uid_t u, gid_t g ){
    FLAT_ENTER;
    ret = Util::Chown(path.c_str(),u,g);
    FLAT_EXIT(ret);
}

int 
flatfile_chmod( const char *logical, mode_t mode ) {
    FLAT_ENTER;
    ret = Util::Chmod(path.c_str(),mode);
    FLAT_EXIT(ret);
}

int 
flatfile_access( const char *logical, int mask ) {
    FLAT_ENTER;
    ret = Util::Access(path.c_str(),mask);
    FLAT_EXIT(ret);
}

// oh.  This might not work across backends on different volumes....
int 
flatfile_rename( const char *logical, const char *to ) {
    FLAT_ENTER; 
    FLAT_EXPAND_TO;
    ret = Util::Rename(old_canonical.c_str(),new_canonical.c_str());
    FLAT_EXIT(ret);
}

ssize_t 
flatfile_read( Plfs_fd *pfd, char *buf, size_t size, off_t offset ) {
    return (ssize_t)-ENOSYS;
}

int 
flatfile_open(Plfs_fd **pfd,const char *logical,int flags,pid_t pid,
        mode_t mode, Plfs_open_opt *open_opt) 
{
    return (ssize_t)-ENOSYS ;
}

int 
flatfile_link(const char *logical, const char *to) {
    FLAT_ENTER;
    FLAT_EXPAND_TO;
    ret = Util::Link(old_canonical.c_str(),new_canonical.c_str());
    return 0;
}

int 
flatfile_utime( const char *logical, struct utimbuf *ut ) {
    FLAT_ENTER;
    ret = Util::Utime(path.c_str(),ut);
    FLAT_EXIT(ret);
}

ssize_t 
flatfile_write(Plfs_fd *pfd, const char *buf, size_t size, 
        off_t offset, pid_t pid)
{
    return (ssize_t)-ENOSYS;
}

int 
flatfile_sync( Plfs_fd *pfd, pid_t pid ) {
    return (ssize_t)-ENOSYS;
}

int 
flatfile_getattr(Plfs_fd *of, const char *logical, 
        struct stat *stbuf,int sz_only)
{
    FLAT_ENTER;
    ret = Util::Stat(path.c_str(),stbuf);
    FLAT_EXIT(ret);
}

int 
flatfile_trunc(Plfs_fd *of, const char *logical, 
    off_t offset, int open_file) 
{
    FLAT_ENTER;
    if (of) of->truncate(offset);
    ret = Util::Truncate(path.c_str(),offset);
    FLAT_EXIT(ret);
}

int 
flatfile_unlink( const char *logical ) {
    FLAT_ENTER;
    ret = Util::Unlink(path.c_str());
    FLAT_EXIT(ret);
}

int 
flatfile_close( Plfs_fd *pfd, pid_t pid, uid_t uid, int open_flags, 
            Plfs_close_opt *close_opt ) 
{
    return -ENOSYS;
}
