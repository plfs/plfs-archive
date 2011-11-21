#include "FlatFile.h"
#include "Util.h"
#include "plfs_private.h"
#include "OpenFile.h"
#include "Metadata.h"

#define FLAT_ENTER PLFS_ENTER

#define FLAT_EXIT(X) if (X<0) X = -errno; PLFS_EXIT(X);

int 
FlatFile::create( const char *logical, mode_t mode, int flags, pid_t pid ){
    FLAT_ENTER;
    ret = Util::Creat(path.c_str(),mode);
    FLAT_EXIT(ret);
}

FlatFile::FlatFile() {
    fd = -1;
    refs = 0;
}

int 
FlatFile::chown( const char *logical, uid_t u, gid_t g ){
    FLAT_ENTER;
    ret = Util::Chown(path.c_str(),u,g);
    FLAT_EXIT(ret);
}

int 
FlatFile::chmod( const char *logical, mode_t mode ) {
    FLAT_ENTER;
    ret = Util::Chmod(path.c_str(),mode);
    FLAT_EXIT(ret);
}

int 
FlatFile::referenceCount() {
    return -ENOSYS;
}

int 
FlatFile::access( const char *logical, int mask ) {
    FLAT_ENTER;
    ret = Util::Access(path.c_str(),mask);
    FLAT_EXIT(ret);
}

// oh.  This might not work across backends on different volumes....
int 
FlatFile::rename( const char *logical, const char *to ) {
    FLAT_ENTER; 
    EXPAND_TARGET;
    ret = Util::Rename(old_canonical.c_str(),new_canonical.c_str());
    FLAT_EXIT(ret);
}

ssize_t 
FlatFile::read( Plfs_fd *pfd, char *buf, size_t size, off_t offset ) {
    int ret = Util::Pread(fd,buf,size,offset);
    return ret; 
}

int 
FlatFile::open(Plfs_fd **pfd,const char *logical,int flags,pid_t pid,
        mode_t mode, Plfs_open_opt *open_opt) 
{
    return (ssize_t)-ENOSYS ;
}

int 
FlatFile::link(const char *logical, const char *to) {
    FLAT_ENTER;
    EXPAND_TARGET;
    ret = Util::Link(old_canonical.c_str(),new_canonical.c_str());
    return 0;
}

int 
FlatFile::utime( const char *logical, struct utimbuf *ut ) {
    FLAT_ENTER;
    ret = Util::Utime(path.c_str(),ut);
    FLAT_EXIT(ret);
}

ssize_t 
FlatFile::write(Plfs_fd *pfd, const char *buf, size_t size, 
        off_t offset, pid_t pid)
{
    int ret = Util::Pwrite(fd,buf,size,offset);
    return ret; 
}

int 
FlatFile::sync( Plfs_fd *pfd, pid_t pid ) {
    return (ssize_t)-ENOSYS;
}

int 
FlatFile::getattr(Plfs_fd *of, const char *logical, 
        struct stat *stbuf,int sz_only)
{
    FLAT_ENTER;
    ret = Util::Stat(path.c_str(),stbuf);
    FLAT_EXIT(ret);
}

int 
FlatFile::trunc(Plfs_fd *of, const char *logical, 
    off_t offset, int open_file) 
{
    FLAT_ENTER;
    if (of) of->truncate(offset);
    ret = Util::Truncate(path.c_str(),offset);
    FLAT_EXIT(ret);
}

int 
FlatFile::unlink( const char *logical ) {
    FLAT_ENTER;
    ret = Util::Unlink(path.c_str());
    FLAT_EXIT(ret);
}

int 
FlatFile::close( Plfs_fd *pfd, pid_t pid, uid_t uid, int open_flags, 
            Plfs_close_opt *close_opt ) 
{
    return -ENOSYS;
}
