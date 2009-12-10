#ifndef __PLFS_H_
#define __PLFS_H_

#include <sys/types.h>

class Plfs_fd;

/*

   All PLFS functions return 0 or -errno

*/

int plfs_access( const char *path, int mask );

int plfs_chmod( const char *path, mode_t mode );

int plfs_close( Plfs_fd * );

int plfs_create( const char *path, mode_t mode, int flags ); 

int plfs_open( Plfs_fd **, const char *path, int flags, int pid, mode_t );

int plfs_read( Plfs_fd *, char *buf, size_t size, off_t offset );

/* Plfs_fd can be NULL */
int plfs_getattr( Plfs_fd *, const char *path, struct stat *stbuf );

int plfs_sync( Plfs_fd * );

/* Plfs_fd can be NULL */
int plfs_trunc( Plfs_fd *, const char *path, off_t offset );

int plfs_unlink( const char *path );

int plfs_utime( const char *path, struct utimbuf *ut );

int plfs_write( Plfs_fd *, const char *buf, size_t size, off_t offset );

#endif
