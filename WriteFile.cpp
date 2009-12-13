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
    this->index           = NULL;
    this->mode            = mode;
}

WriteFile::~WriteFile() {
    fprintf( stderr, "Delete self %s\n", physical_path.c_str() );
    Close();
    if ( index ) {
        closeIndex();
        delete index;
    }
}

int WriteFile::sync( pid_t pid ) {
    int fd, ret; 
    ret = fd = getFd( pid );
    if ( fd >= 0 ) {
        ret = Util::Fsync( fd );
        if ( ret == 0 ) ret = Util::Fsync( index->getFd() );
        if ( ret != 0 ) ret = -errno;
    }
    return ret;
}

int WriteFile::addWriter( pid_t pid ) {
    int ret = 0;
    int fd = openDataFile( physical_path, hostname, pid, mode ); 
    if ( fd < 0 ) {
        ret = -errno;
    } else {
        fds[pid] = fd;
    }
    return ret;
}

int WriteFile::getFd( pid_t pid ) {
    map<pid_t,int>::iterator itr;
    if ( (itr = fds.find( pid )) != fds.end() ) {
        return itr->second;
    } else {
        return -ENOENT;
    }
}

int WriteFile::removeWriter( pid_t pid ) {
    int ret = 0;
    int fd  = getFd( pid );
    if ( fd >= 0 ) {
        ret = Util::Close( fd );
        if ( ret == 0 ) {
            fds.erase( pid );
        }
    } else {
        ret = -ENOENT;
    }
    return ret;
}

int WriteFile::extend( off_t offset ) {
    // make a fake write
    if ( fds.begin() == fds.end() ) return -ENOENT;
    pid_t p = fds.begin()->second;
    double ts = Util::getTime();
    index->addWrite( offset, 0, p, ts, ts );
    addWrite( offset, 0 );   // maintain metadata
    return 0;
}

// we are currently doing synchronous index writing.
// this is were to change it to buffer if you'd like
// returns bytes written or -errno
ssize_t WriteFile::write(const char *buf, size_t size, off_t offset, pid_t pid){
    int ret; ssize_t written;
    int fd  = getFd( pid );
    double begin, end;
    begin = Util::getTime();
    ret = written = ( size ? Util::Write( fd, buf, size ) : 0 );
    end = Util::getTime();
    if ( ret >= 0 ) {
        index->addWrite( offset, ret, pid, begin, end );
        ret = index->flush();
        addWrite( offset, size );   // maintain metadata
    }
    return ( ret == 0 ? written : -errno );
}

// returns 0 or -errno
int WriteFile::openIndex( pid_t pid ) {
    int ret = 0;
    int fd = openIndexFile( physical_path, hostname, pid, mode );
    if ( fd < 0 ) {
        ret = -errno;
    } else {
        index = new Index( physical_path, fd );
    }
    return ret;
}

int WriteFile::closeIndex( ) {
    int ret = 0;
    ret = Util::Close( index->getFd() );
    delete( index );
    index = NULL;
    return ret;
}

// returns 0 or -errno
int WriteFile::Close() {
    int failures = 0;
    map<pid_t,int>::iterator itr;
        // these should already be closed here
        // from each individual pid's close but just in case
    for( itr = fds.begin(); itr != fds.end(); itr++ ) {
        if ( Util::Close( itr->second ) != 0 ) {
            failures++;
        }
    }
    fds.clear();
    return ( failures ? -EIO : 0 );
}

// returns 0 or -errno 
int WriteFile::truncate( off_t offset ) {
    Metadata::truncate( offset );
    index->truncateHostIndex( offset );
    return 0;
}

int WriteFile::openIndexFile( string path, string host, pid_t p, mode_t m ) {
    return openFile( Container::getIndexPath(path.c_str(),host.c_str(), p ), m);
}

int WriteFile::openDataFile(string path, string host, pid_t p, mode_t m){
    return openFile( Container::getDataPath( path.c_str(), host.c_str(), p ),m);
}

// returns an fd or -1
int WriteFile::openFile( string physicalpath, mode_t mode ) {
    int flags = O_WRONLY | O_APPEND | O_CREAT;
    int fd = Util::Open( physicalpath.c_str(), flags, mode );
    fprintf( stderr, "open %s : %d %s\n", physicalpath.c_str(), 
            fd, ( fd < 0 ? strerror(errno) : "" ) );
    if ( fd >= 0 ) paths[fd] = physicalpath;    // remember so restore works
    return fd;
}

// if fuse::f_truncate is used, we will have open handles that get messed up
// in that case, we need to restore them
// return 0 or -errno
int WriteFile::restoreFds( ) {
    map<int,string>::iterator paths_itr;
    map<pid_t, int>::iterator pids_itr;
    int ret = 0;
    
    // first reset the index fd
    if ( index ) {
        index->flush();

        paths_itr = paths.find( index->getFd() );
        if ( paths_itr == paths.end() ) return -ENOENT;
        string indexpath = paths_itr->second;

        if ( Util::Close( index->getFd() ) != 0 )    return -errno;
        if ( (ret = openFile( indexpath, mode )) < 0 ) return -errno;
        index->resetFd( ret );
    }

    // then the data fds
    for( pids_itr = fds.begin(); pids_itr != fds.end(); pids_itr++ ) {

        paths_itr = paths.find( pids_itr->second );
        if ( paths_itr == paths.end() ) return -ENOENT;
        string datapath = paths_itr->second;

        if ( Util::Close( pids_itr->second ) != 0 ) return -errno;
        if ( (ret = openFile( datapath, mode )) < 0 ) return -errno;
    }

    // normally we return ret at the bottom of our functions but this
    // function had so much error handling, I just cut out early on any 
    // error.  therefore, if we get here, it's happy days!
    return 0;
}
