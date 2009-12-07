#include "plfs.h"
#include "Index.h"
#include "WriteFile.h"
#include "Container.h"
#include "Util.h"

int 
plfs_create( const char *path, mode_t mode, int flags ) {
    int attempts = 0;
    return Container::create( path, Util::hostname(), mode, flags, &attempts );
}

// returns bytes read or -errno
int read_helper( Index *index, char *buf, size_t size, off_t offset ) {
    off_t  chunk_offset = 0;
    size_t chunk_length = 0;
    int    fd           = -1;

    // need to lock this since the globalLookup does the open of the fds
    int ret = index->globalLookup( &fd, &chunk_offset, &chunk_length, offset );
    if ( ret != 0 ) return ret;

    size_t read_size = ( size < chunk_length ? size : chunk_length );
    if ( read_size > 0 ) {
        // uses pread to allow the fd's to be shared 
        if ( fd >= 0 ) {
            ret = Util::Pread( fd, buf, read_size, chunk_offset );
            if ( ret < 0 ) {
                cerr << "Couldn't read in " << fd << ":" 
                     << strerror(errno) << endl;
                return -errno;
            }
        } else {
            // zero fill the hole
            memset( (void*)buf, 0, read_size);
            ret = read_size;
        }
    } else {
        // when chunk_length = 0, that means we're at EOF
        ret = 0;
    }
    return ret;
}

int 
plfs_read( Plfs_fd *pfd, char *buf, size_t size, off_t offset ) {
	int ret = 0;
    Index *index = pfd->getIndex(); 
    bool new_index_created = false;  
    const char *path = pfd->getPath();

    // possible that we opened the file as O_RDWR
    // if so, build an index now, but destroy it after this IO
    // so that new writes are re-indexed for new reads
    // basically O_RDWR is possible but it can reduce read BW

    if ( index == NULL ) {
        index = new Index( path, SHARED_PID );
        if ( index ) {
            new_index_created = true;
            ret = Container::populateIndex( path, index );
        } else {
            ret = -EIO;
        }
    }

    // now that we have an index (newly created or otherwise), go ahead and read
    if ( ret == 0 ) {
            // if the read spans multiple chunks, we really need to loop in here
            // to get them all read bec the man page for read says that it 
            // guarantees to return the number of bytes requested up to EOF
            // so any partial read by the client indicates EOF and a client
            // may not retry
        size_t bytes_remaining = size;
        size_t bytes_read      = 0;
        do {
            ret = read_helper( index, &(buf[bytes_read]), bytes_remaining, 
                    offset + bytes_read );
            if ( ret > 0 ) {
                bytes_read      += ret;
                bytes_remaining -= ret;
            }
        } while( bytes_remaining && ret > 0 );
        ret = ( ret < 0 ? ret : bytes_read );
    }

    if ( new_index_created ) {
        delete( index );
    }
    return ret;
}

int
plfs_open( Plfs_fd **pfd, const char *path, int flags, int pid, mode_t mode ) {
    WriteFile *wf    = NULL;
    Index     *index = NULL;
    int ret          = 0;

    // make sure we're allowed to open this container
    // this breaks things when tar is trying to create new files
    // with --r--r--r bec we create it w/ that access and then 
    // we can't write to it
    //ret = checkAccess( strPath, fi ); 

    if ( flags & O_CREAT ) {
        ret = plfs_create( path, mode, flags ); 
    }

    // go ahead and open it here, pass O_CREAT to create it if it doesn't exist 
    // "open it" just means create the data structures we use 
    if ( ret == 0 && (flags & O_WRONLY || flags & O_RDWR) ) {
        wf = new WriteFile( path, Util::hostname(), mode ); 
        wf->setMode ( mode );
    } else if ( ret == 0 ) {
        index = new Index( path, pid );  
        int ret = Container::populateIndex( path, index );
        if ( ret != 0 ) {
            index = NULL;
            fprintf( stderr, "WTF.  getIndex for %s failed\n", path );
        }
    }

    if ( ret == 0 ) {
        *pfd = new OpenFile( wf, index, pid ); 
        (*pfd)->setPath( path );
    }
    return ret;
}

int 
plfs_write( Plfs_fd *pfd, const char *buf, size_t size, off_t offset ) {
    int ret;
    int data_fd, indx_fd;
    Index *index;
    size_t written = -1;

    // the fd's don't get opened in the open, we 
    // wait until the first write to open them.  I forget why.
    // One reason is that if we just touch a file, then we don't 
    // necessarily want empty data and index files.
    // if this seems like the wrong choice, then we can move this
    // code into the open
    pfd->getWriteFds( &data_fd, &indx_fd, &index );
    if ( data_fd < 0 || indx_fd < 0 ) {
        WriteFile *wf = pfd->getWritefile();
        if ( wf == NULL ) {
            errno = EPERM;
            ret = -1;
        }
        int ret;
        for( int attempts = 0; attempts <= 1 && ret == 0; attempts++ ) {
            ret =wf->getFds(pfd->getPid(), O_CREAT, &indx_fd, &data_fd, &index);
            if ( ret == -ENOENT && attempts == 0 ) {
                // we might have to make a host dir if one doesn't exist yet
                ret = Container::makeHostDir( pfd->getPath(), 
                        wf->getHost(), wf->getMode() );
            } else {
                break;
            }
        }
    }

    if ( ret == 0 ) {
        double begin_timestamp = 0, end_timestamp = 0;
        begin_timestamp = Util::getTime();
        ret = Util::Write( data_fd, buf, size );
        end_timestamp   = Util::getTime();
        if ( ret >= 0 ) {
            written = ret;
            pfd->addWrite( offset, written );
        }
        if ( index ) {
            index->addWrite( offset, written, begin_timestamp, end_timestamp );
        } else {
            ret = Index::writeIndex( indx_fd, offset, written, pfd->getPid(),
                begin_timestamp, end_timestamp );
        }
    }

    return ( ret >= 0 ? written : ret );
}

int plfs_sync( Plfs_fd *pfd ) {
    Index *index = NULL;
    int indexfd = -1, datafd = -1, ret = 0;
    pfd->getWriteFds( &datafd, &indexfd, &index );
    if ( index ) {
        ret = index->writeIndex( indexfd );
    }
    if ( ret == 0 ) {
        if ( indexfd > 0 && Util::Fsync( indexfd ) != 0 ) {
            ret = -errno;
        }
        if ( datafd > 0 && Util::Fsync( datafd ) != 0 ) {
            ret = -errno;
        }
    }
    return ret;
}

int 
plfs_trunc( const char *path ) {

}

int 
plfs_unlink( const char *path ) {
}

int
plfs_close( Plfs_fd *pfd ) {
    int ret = 0;
    WriteFile *wf    = pfd->getWritefile();
    Index     *index = pfd->getIndex();
    if ( wf ) {
        off_t  last_offset;
        size_t total_bytes;
        pfd->getMeta( &last_offset, &total_bytes );
        ret = plfs_sync( pfd );  // make sure all data is sync'd
        Container::addMeta( last_offset, total_bytes, pfd->getPath(),
                wf->getHost() );
        ret = wf->Close();
        Container::removeOpenrecord( pfd->getPath(), wf->getHost() );
        delete( wf );
    } else if ( index ) {
        delete( index );
    }
    return ret;
}
