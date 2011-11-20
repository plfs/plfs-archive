#ifndef __FLATFILE_H_
#define __FLATFILE_H_

#include "plfs.h"

int flatfile_create( const char *logical, mode_t mode, int flags, pid_t pid );

int flatfile_chown( const char *logical, uid_t u, gid_t g );

int flatfile_chmod( const char *logical, mode_t mode ) ;

int flatfile_access( const char *logical, int mask ) ;

int flatfile_rename( const char *logical, const char *to ) ;

ssize_t flatfile_read( Plfs_fd *pfd, char *buf, size_t size, off_t offset ) ;

int flatfile_open(Plfs_fd **pfd,const char *logical,int flags,pid_t pid,
        mode_t mode, Plfs_open_opt *open_opt) ;

int flatfile_link(const char *logical, const char *to) ;

int flatfile_utime( const char *logical, struct utimbuf *ut ) ;

ssize_t flatfile_write(Plfs_fd *pfd, const char *buf, size_t size, 
        off_t offset, pid_t pid);

int flatfile_sync( Plfs_fd *pfd, pid_t pid ) ;

int flatfile_getattr(Plfs_fd *of, const char *logical, 
        struct stat *stbuf,int sz_only);

int flatfile_trunc(Plfs_fd *of, const char *logical, 
    off_t offset, int open_file) ;

int flatfile_unlink( const char *logical ) ;

int flatfile_close( Plfs_fd *pfd, pid_t pid, uid_t uid, int open_flags, 
            Plfs_close_opt *close_opt ) ;

#endif
