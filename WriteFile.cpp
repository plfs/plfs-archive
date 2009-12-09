#include "plfs.h"
#include "plfs_private.h"
#include "COPYRIGHT.h"
#include "WriteFile.h"
#include "Container.h"

#include <assert.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/dir.h>
#include <errno.h>
#include <string>
using namespace std;

WriteFile::WriteFile( string path, string hostname, 
        mode_t mode ) : Metadata::Metadata() 
{
    this->physical_path   = path;
    this->hostname        = hostname;
    this->indexfd         = -1;
    this->mode            = mode;
}

WriteFile::~WriteFile() {
    fprintf( stderr, "Delete self %s\n", physical_path.c_str() );
    Close();
}

// returns 0 or -errno
int WriteFile::Close() {
    int failures = 0;
    map<int,FDS>::iterator itr;
        // these should already be closed here
        // from each individual pid's close but just in case
    for( itr = fd_map.begin(); itr != fd_map.end(); itr++ ) {
        failures += closeFds( itr->second, itr->first );
    }
        // but this is shared so no individual pid will close it
        // so close it now 
    if ( this->indexfd > 0 ) {
        failures += closeFd( this->indexfd, "index", SHARED_PID );
        this->indexfd = -1;
    }
    if ( failures == 0 ) {
        fd_map.clear();
    }
    return ( failures ? -EIO : 0 );
}

int WriteFile::closeIndexFile( int fd ) {
    return Util::Close( fd );
}

int WriteFile::closeFd( int fd, const char *type, int pid ) {
    int ret = Util::Close( fd );
    // get the full path here if you want by calling Container::
    fprintf( stderr, "Close %d's %s of %s (%d): %s\n", 
            pid, type, physical_path.c_str(), fd, 
            ( ret == 0 ? "Success" : strerror(errno) ) );
    return ( ret != 0 );
}

int WriteFile::closeFds( FDS fds, int pid ) {
    int failures = 0;
        // don't close the indexfd here, it is shared
    //failures += closeFd( fds.indexfd, "index", pid );
    failures += closeFd( fds.datafd,  "data",  pid );
    if ( fds.index != NULL ) delete( fds.index );
    return failures;
}

int WriteFile::Close( int pid ) {
    map<int,FDS>::iterator itr;
    itr = fd_map.find( pid );
    int failures = 0;
    if ( itr != fd_map.end() ) {
        failures += closeFds( itr->second, itr->first );
    }
        // don't close the shared index pid here
    return ( failures ? -EIO : 0 );
}

int WriteFile::Remove( int pid ) {
    return (int)fd_map.erase( pid );
}

// returns 0 or -errno 
int WriteFile::truncate( off_t offset ) {
    Metadata::truncate( offset );
    map<int,FDS>::iterator itr;
    for( itr = fd_map.begin(); itr != fd_map.end(); itr++ ) {
        if ( itr->second.index != NULL ) {
            itr->second.index->truncateHostIndex( offset );
        }
    }
    return 0;
}

int WriteFile::openIndexFile( string path, string host, mode_t mode ) {
    return openFile( Container::getIndexPath(path.c_str(),host.c_str()), mode );
}

int WriteFile::openDataFile(string path, string host, pid_t pid, mode_t mode){
    return openFile( Container::getDataPath( path.c_str(), host.c_str(), pid ), 
                     mode );
}

// returns 0 or -errno
int WriteFile::getFds( pid_t pid, mode_t mode, 
        int *indexfd, int *datafd, Index **index ) 
{
    FDS fds;
    int ret   = 0;
    map<int,FDS>::iterator itr;
    itr = fd_map.find( pid );
    if ( itr == fd_map.end() ) {
        if ( mode & O_CREAT ) {
            // not found, need to open them and save them and return them
            if ( this->indexfd < 0 ) {
                this->indexfd = openIndexFile( physical_path, hostname, 
                        this->mode );
            }
            fds.indexfd = this->indexfd;
            fds.datafd = openDataFile(physical_path, hostname, pid, this->mode);
            assert( fds.index );
            if ( fds.indexfd < 0 || fds.datafd < 0 ) {
                fprintf( stderr, 
                        "Failed to open pids for %s: %s "
                        "(possible that container existed but not hostdir)\n",
                    physical_path.c_str(), strerror( errno ) );
                ret = -errno;
            } else {
                fds.index = NULL;
                fd_map[pid] = fds;
            }
        } else {
                // not found and not asked to create them
            *indexfd = -1;
            *datafd  = -1;
            *index   = NULL;
            return 0;
        }
    } else {
        fds = itr->second;
    }
    *indexfd = fds.indexfd;
    *datafd  = fds.datafd;
    *index   = fds.index;
    return ret;
}

// returns an fd or -1
int WriteFile::openFile( string physicalpath, mode_t mode ) {
    int flags = O_WRONLY | O_APPEND | O_CREAT;
    int fd = Util::Open( physicalpath.c_str(), flags, mode );
    fprintf( stderr, "open %s : %d %s\n", physicalpath.c_str(), 
            fd, ( fd < 0 ? strerror(errno) : "" ) );
    return fd;
}

