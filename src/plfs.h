#ifndef __PLFS_H_
#define __PLFS_H_

#include <utime.h>
#include <sys/stat.h>
#include <sys/types.h>

#ifdef __cplusplus
class Plfs_fd;
extern "C" {
#else
typedef void Plfs_fd;
#endif


/*

   All PLFS functions return 0 or -errno

   This code does allow for multiple threads to share a single Plfs_fd ptr
   However, thread safety is the responsibility of the caller

*/

int plfs_access( const char *path, int mask );

int plfs_chmod( const char *path, mode_t mode );

int plfs_chown( const char *path, uid_t, gid_t );

int plfs_close( Plfs_fd *, pid_t pid );

/* plfs_create
   you don't need to call this, you can also pass O_CREAT to plfs_open
*/
int plfs_create( const char *path, mode_t mode, int flags ); 

/* plfs_open
*/
int plfs_open( Plfs_fd **, const char *path, 
        int flags, pid_t pid, mode_t );

ssize_t plfs_read( Plfs_fd *, char *buf, size_t size, off_t offset );

/* Plfs_fd can be NULL */
int plfs_getattr( Plfs_fd *, const char *path, struct stat * );

/* individual writers can be sync'd.  */
int plfs_sync( Plfs_fd *, pid_t );

/* Plfs_fd can be NULL, but then path must be valid */
int plfs_trunc( Plfs_fd *, const char *path, off_t );

int plfs_unlink( const char *path );

int plfs_utime( const char *path, struct utimbuf * );

ssize_t plfs_write( Plfs_fd *, const char *, size_t, off_t, pid_t );

#ifdef __cplusplus
}
#endif

#endif
