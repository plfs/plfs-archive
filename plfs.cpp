#include "plfs.h"
#include "plfs_private.h"
#include "Index.h"
#include "WriteFile.h"
#include "Container.h"
#include "Util.h"
#include "OpenFile.h"

// a shortcut for functions that are expecting zero
int 
retValue( int res ) {
    return Util::retValue(res);
}

int 
plfs_create( const char *path, mode_t mode, int flags ) {
    int attempts = 0;
    return Container::create( path, Util::hostname(), mode, flags, &attempts );
}

int
plfs_access( const char *path, int mask ) {
    int ret = -1;
    if ( Container::isContainer( path ) ) {
        ret = Container::Access( path, mask );
    } else {
        ret = retValue( Util::Access( path, mask ) );
    }
    return ret;
}
int 
plfs_chmod( const char *path, mode_t mode ) {
  if ( isContainer( path ) ) {
        ret = retValue( Container::Chmod( path, mode ) );
    } else {
        ret = retValue( Util::Chmod( full.c_str(), mode ) );
    }
}


int
plfs_utime( const char *path, struct utimbuf *ut ) {
    int ret = -1;
    if ( Container::isContainer( path ) ) {
        ret = Container::Utime( path, ut );
    } else {
        ret = retValue( Util::Utime( path, ut ) );
    }
    return ret;
}

// returns bytes read or -errno
int 
read_helper( Index *index, char *buf, size_t size, off_t offset ) {
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
plfs_read( PlfsFd *pfd, char *buf, size_t size, off_t offset ) {
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
plfs_open( PlfsFd **pfd, const char *path, int flags, int pid, mode_t mode ) {
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

    if ( flags & O_TRUNC ) {
        ret = plfs_trunc( NULL, path, 0 );
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
        *pfd = new PlfsFd( wf, index, pid ); 
        (*pfd)->setPath( path );
    }
    return ret;
}

int 
plfs_write( PlfsFd *pfd, const char *buf, size_t size, off_t offset ) {
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

int 
plfs_sync( PlfsFd *pfd ) {
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

// this can fail due to silly rename 
// imagine an N-1 normal file on PanFS that someone is reading and
// someone else is unlink'ing.  The unlink will see a reference count
// and will rename it to .panfs.blah.  The read continues and when the
// read releases the reference count on .panfs.blah drops to zero and
// .panfs.blah is unlinked
// but in our containers, here's what happens:  a chunk or an index is
// open by someone and is unlinked by someone else, the silly rename 
// does the same thing and now the container has a .panfs.blah file in
// it.  Then when we try to remove the directory, we get a ENOTEMPTY.
// truncate only removes the droppings but leaves the directory structure
// 
// we also use this function to implement truncate on a container
// in that case, we remove all droppings but preserve the container
// an empty container = empty file
int 
removeDirectoryTree( const char *path, bool truncate_only ) {
    DIR *dir;
    struct dirent *ent;
    int ret = 0;
    fprintf( stderr, "%s on %s\n", __FUNCTION__, path );

    dir = opendir( path );
    if ( dir == NULL ) return -errno;

    while( (ent = readdir( dir ) ) != NULL ) {
        if ( ! strcmp(ent->d_name, ".") || ! strcmp(ent->d_name, "..") ) {
            //fprintf( stderr, "skipping %s\n", ent->d_name );
            continue;
        }
        if ( ! strcmp(ent->d_name, ACCESSFILE ) && truncate_only ) {
            continue;   // don't remove our accessfile!
        }
        if ( ! strcmp( ent->d_name, OPENHOSTDIR ) && truncate_only ) {
            continue;   // don't remove open hosts
        }
        string child( path );
        child += "/";
        child += ent->d_name;
        if ( Util::isDirectory( child.c_str() ) ) {
            if ( removeDirectoryTree( child.c_str(), truncate_only ) != 0 ) {
                ret = -errno;
                continue;
            }
        } else {
            //fprintf( stderr, "unlinking %s\n", ent->d_name );
            if ( Util::Unlink( child.c_str() ) != 0 ) {
                // ENOENT would be OK, could mean that an openhosts dropping
                // was removed on another host or something
                if ( errno != ENOENT ) {
                    ret = -errno;
                    continue;
                }
            }
        }
    }
    if ( closedir( dir ) != 0 ) {
        ret = -errno;
    }

    // here we have deleted all children.  Now delete the directory itself
    // if truncate is called, this means it's a container.  so we only
    // delete the internal stuff but not the directory structure
    if ( ret != 0 ) return ret;
    if ( truncate_only ) {
        return 0;
    } else {
        ret = Util::Rmdir( path );
        if ( ret != 0 && errno == ENOTEMPTY ) {
            int prec = numeric_limits<long double>::digits10; // 18
            ostringstream trash_path;
            trash_path.precision(prec); // override the default of 6
            trash_path << "." << path << "." << Util::hostname() << "." 
                       << Util::getTime();
            cerr << "Need to silly rename " << path << " to "
                 << trash_path.str().c_str() << endl;
            Util::Rename( path, trash_path.str().c_str() ); 
            ret = 0;
        }
        return retValue( ret );
    }
}
            
// this should only be called if the uid has already been checked
// and is allowed to access this file
// PlfsFd can be NULL
// returns 0 or -errno
int 
plfs_getattr( PlfsFd *of, const char *path, struct stat *stbuf ) {
    int ret = 0;
    if ( ! Container::isContainer( path ) ) {
        ret = retValue( Util::Lstat( path, stbuf ) );
    } else {
        ret = Container::getattr( path, stbuf );
        if ( ret == 0 ) {
            // is it also open currently?
            // we might be reading from some other users writeFile but
            // we're only here if we had access to stat the container
            // commented out some stuff that I thought was maybe slow
            // this might not be necessary depending on how we did 
            // the stat.  If we stat'ed data droppings then we don't
            // need to do this but it won't hurt.  
            // If we were trying to read index droppings
            // and they weren't available, then we should do this.
            WriteFile *wf=(of && of->getWritefile() ? of->getWritefile() :NULL);
            if ( wf ) {
                off_t  last_offset;
                size_t total_bytes;
                wf->getMeta( &last_offset, &total_bytes );
                if ( last_offset > stbuf->st_size ) {
                    stbuf->st_size = last_offset;
                }
                    // if the index has already been flushed, then we might
                    // count it twice here.....
                stbuf->st_blocks += Container::bytesToBlocks(total_bytes);
            }
        }
    }
    return ret;
}

// the PlfsFd can be NULL
int 
plfs_trunc( PlfsFd *of, const char *path, off_t offset ) {
    if ( ! Container::isContainer( path ) ) {
        // this is weird, we expect only to operate on containers
        return retValue( truncate(path,offset) );
    }

    int ret = 0;
    if ( offset == 0 ) {
            // this is easy, just remove all droppings
        ret = removeDirectoryTree( path, true );
    } else {
            // either at existing end, before it, or after it
        struct stat stbuf;
        ret = plfs_getattr( of, path, &stbuf );
        if ( ret == 0 ) {
            if ( stbuf.st_size == offset ) {
                ret = 0; // nothing to do
            } else if ( stbuf.st_size > offset ) {
                ret = Container::Truncate( path, offset ); // make smaller
            } else if ( stbuf.st_size < offset ) {
                // TODO
                //ret = extendFile( of, path, offset );    // make bigger
            }
        }
    }

    // if we actually modified the container, update any open file handle
    if ( ret == 0 && of && of->getWritefile() ) {
        ret = of->getWritefile()->truncate( offset );
        of->truncate( offset );
    }

    return ret;
}

/*
// this is called when truncate has been used to extend a file
// returns 0 or -errno
int Plfs::extendFile( PlfsFd *of, string strPath, off_t offset ) {
    int ret = 0;
    int fd = -1;
    bool newly_opened = false;
    if ( of && of->getWritefile() ) {
            // hey, if this if statement is ever not true,
            // then it looks like we'll get metadata wrong....
        fd = of->getWritefile()->getIndexFd();
        of->getWritefile()->addWrite( offset, 0 ); // to set the metadata
    }
    if ( fd == -1 ) {
        fd = WriteFile::openIndexFile( strPath, 
                (shared.myhost).c_str(), getMode(strPath) );
        if ( fd < 0 ) {
            ret = -errno;
        } else {
            newly_opened = true;
        }
    }
    if ( fd >= 0 ) {
        pid_t pid = of->getPid();
        double begin_timestamp = Util::getTime();
        Util::MutexLock( &shared.index_mutex );
        ret = Index::writeIndex( fd, offset, 0, pid,
                begin_timestamp, begin_timestamp );
        Util::MutexUnlock( &shared.index_mutex );
        if ( newly_opened ) {
            if ( WriteFile::closeIndexFile( fd ) != 0 ) {
                ret = -errno;
            }
        }
    }
    return ret;
}
*/

int 
plfs_unlink( const char *path ) {
    int ret = 0;
    if ( Container::isContainer( path ) ) {
        ret = removeDirectoryTree( path, false );  
    } else {
        // this is weird, we should only be asked to remove containers
        ret = retValue( unlink( path ) );   // recurse
    }
    return ret; 
}

int
plfs_close( PlfsFd *pfd ) {
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
    delete( pfd );
    return ret;
}
