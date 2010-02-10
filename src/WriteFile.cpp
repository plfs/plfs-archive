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
    this->physical_path     = path;
    this->hostname          = hostname;
    this->index             = NULL;
    this->mode              = mode;
    this->index_mux         = NULL;
    // if synchronous index is false, writes will work
    // but O_RDWR reads won't work, and not sure about
    // stat'ing an open file
    this->synchronous_index = true;
    this->writers           = 0;
    pthread_mutex_init( &data_mux, NULL );
}

WriteFile::~WriteFile() {
    Util::Debug( stderr, "Delete self %s\n", physical_path.c_str() );
    Close();
    if ( index ) {
        closeIndex();
        delete index;
    }
}

int WriteFile::sync( pid_t pid ) {
    int ret; 
    OpenFd *ofd = getFd( pid );
    if ( ofd == NULL ) {
        ret = -ENOENT;
    } else {
        ret = Util::Fsync( ofd->fd );
        if ( ret == 0 ) index->flush();
        if ( ret == 0 ) ret = Util::Fsync( index->getFd() );
        if ( ret != 0 ) ret = -errno;
    }
    return ret;
}


// returns -errno or number of writers
int WriteFile::addWriter( pid_t pid ) {
    int ret = 0;

    Util::MutexLock(   &data_mux, __FUNCTION__ );
    struct OpenFd *ofd = getFd( pid );
    if ( ofd ) {
        ofd->writers++;
    } else {
        int fd = openDataFile( physical_path, hostname, pid, mode ); 
        if ( fd >= 0 ) {
            ofd = new struct OpenFd;
            ofd->writers = 1;
            ofd->fd = fd;
            fds[pid] = ofd;
        } else {
            ret = -errno;
        }
    }
    if ( ret == 0 ) ret = ++writers;
    if ( writers > 1 ) {
        index_mux = new pthread_mutex_t;
        pthread_mutex_init( index_mux, NULL );
    }

    Util::Debug( stderr, "%s on %s now has %d writers\n", 
            __FUNCTION__, physical_path.c_str(), writers );
    Util::MutexUnlock( &data_mux, __FUNCTION__ );
    return ret;
}

size_t WriteFile::numWriters( ) {
    return writers; 
}

struct OpenFd * WriteFile::getFd( pid_t pid ) {
    map<pid_t,OpenFd*>::iterator itr;
    struct OpenFd *ofd = NULL;
    if ( (itr = fds.find( pid )) != fds.end() ) {
        ostringstream oss;
        oss << __FILE__ << ":" << __FUNCTION__ << " found fd " 
            << itr->second->fd << " with writers " 
            << itr->second->writers
            << " from pid " << pid << endl;
        Util::Debug( stderr, "%s", oss.str().c_str() );
        ofd = itr->second;
    } else {
        /*
           // I think this code is a mistake.  We were doing it once
           // when a child was writing to a file that the parent opened
           // but shouldn't it be OK to just give the child a new datafile?
        if ( fds.size() > 0 ) {
            ostringstream oss;
            // ideally instead of just taking a random pid, we'd rather
            // try to find the parent pid and look for it
            // we need this code because we've seen in FUSE that an open
            // is done by a parent but then a write comes through as the child
            Util::Debug( stderr, "%s WARNING pid %d is not mapped. "
                    "Borrowing fd %d from pid %d\n",
                    __FILE__, (int)pid, (int)fds.begin()->second->fd,
                    (int)fds.begin()->first );
            ofd = fds.begin()->second;
        } else {
            Util::Debug(stderr, "%s no fd to give to %d\n", __FILE__, (int)pid);
            ofd = NULL;
        }
        */
        Util::Debug(stderr, "%s no fd to give to %d\n", __FILE__, (int)pid);
        ofd = NULL;
    }
    return ofd;
}

int WriteFile::closeFd( int fd ) {
    map<int,string>::iterator paths_itr;
    paths_itr = paths.find( fd );
    string path = ( paths_itr == paths.end() ? "ENOENT?" : paths_itr->second );
    int ret = Util::Close( fd );
    Util::Debug( stderr, "%s:%s closed fd %d for %s: %d %s\n",
            __FILE__, __FUNCTION__, fd, path.c_str(), ret, 
            ( ret != 0 ? strerror(errno) : "success" ) );
    paths.erase ( fd );
    return ret;
}

// returns -errno or number of writers
int WriteFile::removeWriter( pid_t pid ) {
    int ret = 0;
    Util::MutexLock(   &data_mux , __FUNCTION__);
    struct OpenFd *ofd = getFd( pid );
    if ( ofd == NULL ) {
        ret = -ENOENT;
    } else {
        writers--;  ofd->writers--;
        if ( ofd->writers <= 0 ) {
            ret = closeFd( ofd->fd );
            fds.erase( pid );
            delete( ofd );
        }
    }
    Util::Debug( stderr, "%s on %s now has %d writers: %d\n", 
            __FUNCTION__, physical_path.c_str(), writers, ret );
    Util::MutexUnlock( &data_mux, __FUNCTION__ );
    return ( ret == 0 ? writers : ret );
}

int WriteFile::extend( off_t offset ) {
    // make a fake write
    if ( fds.begin() == fds.end() ) return -ENOENT;
    pid_t p = fds.begin()->first;
    double ts = Util::getTime();
    index->addWrite( offset, 0, p, ts, ts );
    addWrite( offset, 0 );   // maintain metadata
    return 0;
}

// we are currently doing synchronous index writing.
// this is where to change it to buffer if you'd like
// returns bytes written or -errno
ssize_t WriteFile::write(const char *buf, size_t size, off_t offset, pid_t pid){
    int ret = 0; 
    ssize_t written;
    OpenFd *ofd = getFd( pid );
    if ( ofd == NULL ) {
        // we used to return -ENOENT here but we can get here legitimately 
        // when a parent opens a file and a child writes to it.
        // so when we get here, we need to add a child datafile
        ret = addWriter( pid );
        if ( ret > 0 ) {
            ofd = getFd( pid );
        }
    }
    if ( ofd && ret >= 0 ) {
        int fd = ofd->fd;
        // write the data file
        double begin, end;
        begin = Util::getTime();
        ret = written = ( size ? Util::Write( fd, buf, size ) : 0 );
        end = Util::getTime();

        // then the index
        if ( ret >= 0 ) {
            if ( index_mux ) Util::MutexLock(   index_mux , __FUNCTION__);
            index->addWrite( offset, ret, pid, begin, end );
            if ( synchronous_index ) ret = index->flush();  
            if ( ret >= 0 )          addWrite( offset, size ); // metadata
            if ( index_mux ) Util::MutexUnlock( index_mux, __FUNCTION__ );
        }
    }

    // return bytes written or error
    return ( ret >= 0 ? written : -errno );
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
    ret = index->flush();
    ret = closeFd( index->getFd() );
    delete( index );
    index = NULL;
    return ret;
}

// returns 0 or -errno
int WriteFile::Close() {
    int failures = 0;

    Util::MutexLock(   &data_mux , __FUNCTION__);
    map<pid_t,OpenFd *>::iterator itr;
        // these should already be closed here
        // from each individual pid's close but just in case
    for( itr = fds.begin(); itr != fds.end(); itr++ ) {
        if ( closeFd( itr->second->fd ) != 0 ) {
            failures++;
        }
    }
    fds.clear();
    Util::MutexUnlock( &data_mux, __FUNCTION__ );

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
    Util::Debug( stderr, "open %s : %d %s\n", physicalpath.c_str(), 
            fd, ( fd < 0 ? strerror(errno) : "" ) );
    if ( fd >= 0 ) paths[fd] = physicalpath;    // remember so restore works
    return ( fd >= 0 ? fd : -errno );
}

// if fuse::f_truncate is used, we will have open handles that get messed up
// in that case, we need to restore them
// return 0 or -errno
int WriteFile::restoreFds( ) {
    map<int,string>::iterator paths_itr;
    map<pid_t, OpenFd *>::iterator pids_itr;
    int ret = 0;
    
    // first reset the index fd
    if ( index ) {
        index->flush();

        paths_itr = paths.find( index->getFd() );
        if ( paths_itr == paths.end() ) return -ENOENT;
        string indexpath = paths_itr->second;

        if ( closeFd( index->getFd() ) != 0 )    return -errno;
        if ( (ret = openFile( indexpath, mode )) < 0 ) return -errno;
        index->resetFd( ret );
    }

    // then the data fds
    for( pids_itr = fds.begin(); pids_itr != fds.end(); pids_itr++ ) {

        paths_itr = paths.find( pids_itr->second->fd );
        if ( paths_itr == paths.end() ) return -ENOENT;

        string datapath = paths_itr->second;
        if ( closeFd( pids_itr->second->fd ) != 0 ) return -errno;

        pids_itr->second->fd = openFile( datapath, mode );
        if ( pids_itr->second->fd < 0 ) return -errno;
    }

    // normally we return ret at the bottom of our functions but this
    // function had so much error handling, I just cut out early on any 
    // error.  therefore, if we get here, it's happy days!
    return 0;
}
