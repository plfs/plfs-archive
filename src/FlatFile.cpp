#include "FlatFile.h"
#include "Util.h"
#include "plfs_private.h"

int 
flatfile_create( const char *logical, mode_t mode, int flags, pid_t pid ){
    PLFS_ENTER;
    ret = Util::Creat(path.c_str(),mode);
    PLFS_EXIT(ret);
}

int 
flatfile_chown( const char *logical, uid_t u, gid_t g ){
    PLFS_ENTER;
    ret = Util::Chown(path.c_str(),u,g);
    PLFS_EXIT(ret);
}

int 
flatfile_chmod( const char *logical, mode_t mode ) {
    PLFS_ENTER;
    ret = Util::Chmod(path.c_str(),mode);
    PLFS_EXIT(ret);
}

int 
flatfile_access( const char *logical, int mask ) {
    PLFS_ENTER;
    ret = Util::Access(path.c_str(),mask);
    PLFS_EXIT(ret);
}

int 
flatfile_rename( const char *logical, const char *to ) {
    PLFS_ENTER; (void)ret;
    string old_canonical = path;
    string new_canonical;
    ExpansionInfo exp_info;
    new_canonical = expandPath(to,&exp_info,EXPAND_CANONICAL,-1,0);
    return 0;
}

ssize_t 
flatfile_read( Plfs_fd *pfd, char *buf, size_t size, off_t offset ) {
    return 0;
}

int 
flatfile_open(Plfs_fd **pfd,const char *logical,int flags,pid_t pid,
        mode_t mode, Plfs_open_opt *open_opt) 
{
    return 0;
}

int 
flatfile_link(const char *logical, const char *to) {
    return 0;
}

int 
flatfile_utime( const char *logical, struct utimbuf *ut ) {
    return 0;
}

ssize_t 
flatfile_write(Plfs_fd *pfd, const char *buf, size_t size, 
        off_t offset, pid_t pid)
{
    return 0;
}

int 
flatfile_sync( Plfs_fd *pfd, pid_t pid ) {
    return 0;
}

int 
flatfile_getattr(Plfs_fd *of, const char *logical, 
        struct stat *stbuf,int sz_only)
{
    return 0;
}

int 
flatfile_trunc(Plfs_fd *of, const char *logical, 
    off_t offset, int open_file) 
{
    return 0;
}

int 
flatfile_unlink( const char *logical ) {
    return 0;
}

int 
flatfile_close( Plfs_fd *pfd, pid_t pid, uid_t uid, int open_flags, 
            Plfs_close_opt *close_opt ) 
{
    return 0;
}
