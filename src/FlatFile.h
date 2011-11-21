#ifndef __FLATFILE_H_
#define __FLATFILE_H_

#include "LogicalFile.h"

class 
FlatFile : public LogicalFile {
    public:

        // here are the methods for creating an instatiated object
        // no destructor needed
        FlatFile(); 
        int open(Plfs_fd **pfd,const char *logical,int flags,pid_t pid,
                    mode_t mode, Plfs_open_opt *open_opt);
        ssize_t read(Plfs_fd *pfd, char *buf, size_t size, off_t offset);
        ssize_t write(Plfs_fd *pfd, const char *buf, size_t size, 
                off_t offset, pid_t pid);
        int close( Plfs_fd *pfd, pid_t pid, uid_t uid, int open_flags, 
            Plfs_close_opt *close_opt );
        int referenceCount();

        // here are a bunch of methods for operating on one
        // these should be static but there aren't static virtual methods
        int chown( const char *logical, uid_t u, gid_t g );
        int chmod( const char *logical, mode_t mode );
        int access( const char *logical, int mask );
        int rename(const char *logical, const char *to);
        int link(const char *logical, const char *to);
        int utime( const char *logical, struct utimbuf *ut );
        int sync( Plfs_fd *pfd, pid_t pid );
        int getattr(Plfs_fd *of, const char *logical, 
                    struct stat *stbuf,int sz_only);
        int trunc(Plfs_fd *of, const char *logical, 
                    off_t offset, int open_file);
        int unlink( const char *logical );
        int create(const char *logical, mode_t, int flags, pid_t pid);

        bool needsSeparateTrunc() {return false;}
        bool needsSeparateCreate() {return false;}
    private:
        int fd;
        int refs; // track reference counting
};


#endif
