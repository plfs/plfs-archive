#ifndef __LOGICALFILE_H_
#define __LOGICALFILE_H_

#include "plfs.h"

#define LOGICALFILE_ENTER PLFS_ENTER

#define LOGICALFILE_EXIT(X) if (X<0) X = -errno; PLFS_EXIT(X);

#define EXPAND_TARGET \
    string old_canonical = path; \
    string new_canonical; \
    ExpansionInfo exp_info; \
    new_canonical = expandPath(to,&exp_info,EXPAND_CANONICAL,-1,0);

// our pure virtual class for which we currently have FlatFile and we need
// ContainerFile


class 
LogicalFile {

    public:
        // here are the methods for creating an instatiated object
        // no destructor needed
        virtual int open(Plfs_fd **pfd,const char *logical,int flags,pid_t pid,
                    mode_t mode, Plfs_open_opt *open_opt);
        virtual ssize_t read(Plfs_fd *, char *, size_t, off_t);
        virtual ssize_t write(Plfs_fd *, const char *, size_t, off_t, pid_t);
        virtual int close(Plfs_fd *, pid_t, uid_t, int open_flags, 
            Plfs_close_opt *close_opt );
        virtual int referenceCount();
        virtual ~LogicalFile();

        // here are a bunch of methods for operating on one
        // these could be virtual except virtual classes can't be virtual
        virtual int chown( const char *logical, uid_t u, gid_t g );
        virtual int chmod( const char *logical, mode_t mode );
        virtual int access( const char *logical, int mask );
        virtual int rename(const char *logical, const char *to);
        virtual int link(const char *logical, const char *to);
        virtual int utime( const char *logical, struct utimbuf *ut );
        virtual int sync( Plfs_fd *pfd, pid_t pid );
        virtual int getattr(Plfs_fd *of, const char *logical, 
                    struct stat *stbuf,int sz_only);
        virtual int trunc(Plfs_fd *of, const char *logical, 
                    off_t offset, int open_file);
        virtual int unlink( const char *logical );
        virtual int create(const char *logical, mode_t, int flags, pid_t pid);

        // here are some to provide info about how to use the class
        virtual bool needsSeparateTrunc();
        virtual bool needsSeparateCreate();
};


#endif
