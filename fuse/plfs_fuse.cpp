#include "plfs.h"
#include "Util.h"
#include "Index.h"
#include "OpenFile.h"
#include "WriteFile.h"
#include "Container.h"
#include "LogMessage.h"
#include "COPYRIGHT.h"

#include <errno.h>
#include <string>
#include <fstream>
#include <iostream>
#include <fcntl.h>
#include <iostream>
#include <limits>
#include <assert.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/dir.h>
#include <sys/syscall.h>
#include <sys/param.h>
#include <sys/mount.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <time.h>
#include <iomanip>
#include <iostream>
#include <sstream>
#include "plfs_fuse.h"
#include "fusexx.h"

using namespace std;

#define DEBUGFILE ".plfsdebug"
#define DEBUGLOG  ".plfslog"
#define DEBUGFILESIZE 1048576
#define DANGLE_POSSIBILITY 1

#ifdef PLFS_TIMES
    #define START_TIMES double begin, end;  \
                        begin = Util::getTime();
    #define END_TIMES   end = Util::getTime(); \
                        Util::addTime( __FUNCTION__, end-begin );
#else
    #define START_TIMES
    #define END_TIMES
#endif

#ifdef __FreeBSD__
    #define SAVE_IDS
    #define RESTORE_IDS 
#else
    #include <sys/fsuid.h>  
    #define SAVE_IDS   uid_t save_uid = Util::Getuid();                   \
                       gid_t save_gid = Util::Getgid();                   \
                       Util::Setfsuid( fuse_get_context()->uid );         \
                       Util::Setfsgid( fuse_get_context()->gid );

    #define RESTORE_IDS Util::Setfsuid(save_uid); Util::Setfsgid(save_gid);
#endif


#define PLFS_ENTER string strPath  = expandPath( path );               \
                   ostringstream funct_id;                             \
                   LogMessage lm;                                      \
                   START_TIMES;                                        \
                   funct_id << setw(16) << fixed << setprecision(16)   \
                        << begin << " PLFS::" << __FUNCTION__          \
                        << " on " << strPath << " ";                   \
                   lm << funct_id.str() << endl;                       \
                   lm.flush();                                         \
                   SAVE_IDS;                                           \
                   int ret = 0;

#define PLFS_ENTER_IO  PLFS_ENTER
#define PLFS_ENTER_PID PLFS_ENTER 
#define PLFS_EXIT      RESTORE_IDS; END_TIMES;                            \
                       funct_id << (ret ? strerror(errno) : "success")   \
                                << " " << end-begin << "s";               \
                       lm << funct_id.str() << endl; lm.flush();          \
                       return ret;

#define PLFS_EXIT_IO   PLFS_EXIT 

#define EXIT_IF_DEBUG  if ( isdebugfile(path) ) return 0;

#define SETUP_OPEN_FILES    PlfsFd  *of    = (PlfsFd*)fi->fh;   \
                            WriteFile *wf    = NULL;                \
                            Index     *index = NULL;                \
                            if ( of ) {                             \
                                wf    = of->getWritefile();         \
                                index = of->getIndex();             \
                            }



// the declaration of our shared state container
SharedState shared;

std::vector<std::string> &
split(const std::string &s, const char delim, std::vector<std::string> &elems) {
    std::stringstream ss(s);
    std::string item;
    while(std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}

// a utility for discovering whether something is a directory
// we need to know this since file ops are handled by PLFS
// but directory ops need to be handled here since we have
// the multiple backend feature
bool Plfs::isDirectory( string path ) {
    bool isdir = false;
    struct stat buf;
    if ( plfs_getattr( path.c_str(), &buf ) == 0 ) {
        isdir = ( buf.st_mode & S_IFDIR );
    }
    return isdir;
}

// set this up to parse command line args
// move code from constructor in here
// and stop using /etc config file
int Plfs::init( int *argc, char **argv ) {
        // figure out our hostname now in order to make containers
    char hostname[PPATH];
    if (gethostname(hostname, sizeof(hostname)) < 0) {
        fprintf(stderr,"plfsfuse gethostname failed");
        return -errno;
    }
    shared.myhost = hostname; 

    cerr << "Starting PLFS on " << hostname << "." << endl;
    LogMessage::init( "/dev/stderr" );

        // bufferindex works and is good because it consolidates the index some
        // but the problem is that make test doesn't work on my mac book air.
    shared.params.bufferindex   = true;
    shared.params.subdirs       = 32;
    shared.params.sync_on_close = false;

        // modify argc to remove -plfs args so that fuse proper doesn't see them
    int removed_args = 0;

        // parse args
    for( int i = 0; i < *argc; i++ ) {
        const char *value;
        if ( (value = getPlfsArg( "plfs_backend", argv[i] )) ) {
            string fullvalue( value );
            split( fullvalue, ',', shared.params.backends );
            removed_args++;
        } 
        if ( (value = getPlfsArg( "plfs_bufferindex", argv[i]) ) ) {
            shared.params.bufferindex = atoi( value );
            removed_args++;
        }
        if ( (value = getPlfsArg( "plfs_synconclose", argv[i]) ) ) {
            shared.params.sync_on_close = atoi( value );
            removed_args++;
        }
        if ( (value = getPlfsArg( "plfs_subdirs", argv[i] )) ) {
            shared.params.subdirs = atoi( value );
            removed_args++;
        }
    }
    *argc -= removed_args;

        // make sure our backend store is good
    if (!shared.params.backends.size()) {
        cerr << "FATAL: No valid backend directory found.  "
             << "Pass -plfs_backend=/path/to/backend\n"; 
        return -ENOENT;
    }

    vector<string>::iterator itr;
    for(itr = shared.params.backends.begin(); 
        itr != shared.params.backends.end(); 
        itr++)
    {
        if ( ! isDirectory( *itr ) ) {
            cerr << "FATAL: " << *itr << " is not a valid backend directory.  "
                 << "Pass -plfs_backend=/path/to/backend\n"; 
            return -ENOENT;
        }
   }

        // create a dropping so we know when we start   
    int fd = open( "/tmp/plfs.starttime",
            O_WRONLY | O_APPEND | O_CREAT, DEFAULT_MODE );
    char buffer[1024];
    snprintf( buffer, 1024, "PLFS started at %.2f\n", Util::getTime() );
    write( fd, buffer, 1024 );
    close( fd );

        // init our mutex
    pthread_mutex_init( &(shared.container_mutex), NULL );
    pthread_mutex_init( &(shared.fd_mutex), NULL );
    pthread_mutex_init( &(shared.index_mutex), NULL );

        // make sure we have a trash container
    f_mkdir( TRASHDIR, DEFAULT_MODE );

        // seed our random number generator which we use to know when to link
        // in dangling files
    srand(time(NULL));

    return 0;
}

const char * Plfs::getPlfsArg( const char *key, const char *arg ) {
    if ( strstr( arg, key ) ) {
        const char *equal = strstr( arg, "=" );
        return ( equal == NULL ? NULL : ++equal );
    } else {
        return NULL;
    }
}

// Constructor
Plfs::Plfs () {
    extra_attempts      = 0;
    wtfs                = 0;
    make_container_time = 0;
    o_rdwrs             = 0;
    #ifdef COUNT_SKIPS
        fward_skips = bward_skips = nonskip_writes = 0;
    #endif
    begin_time          = Util::getTime();
}

// this takes the logical path, and returns the path to this object on the
// underlying mount point
// hash the path and choose a backend
// hash on path means N-N open storm
// hash on host improves this but makes multiple containers
// what we really want is hash on host for the create
// and hash on path for the lookup
string Plfs::expandPath( const char *path ) {
    size_t hash_by_node  = Container::hashValue( shared.myhost.c_str() )
                            % shared.params.backends.size();
    size_t hash_by_path  = Container::hashValue( path )
                            % shared.params.backends.size();
    string dangle_path( shared.params.backends[hash_by_node] + "/" + path );
    string canonical_path( shared.params.backends[hash_by_path] + "/" + path );
    // stop doing the dangling stuff for now
    return canonical_path;
}

bool Plfs::isdebugfile( const char *path ) {
    return ( isdebugfile( path, DEBUGFILE ) || isdebugfile( path, DEBUGLOG ) );
}

bool Plfs::isdebugfile( const char *path, const char *file ) {
    const char *ptr = path;
    if ( ptr[0] == '/' ) {
        ptr++;  // skip past the forward slash
    }
    return ( ! strcmp( ptr, file ) );
}

int Plfs::makePlfsFile( string expanded_path, mode_t mode, int flags ) {
    int res = 0;
    fprintf( stderr, "Need to create container for %s (%s %d)\n", 
            expanded_path.c_str(), 
            shared.myhost.c_str(), fuse_get_context()->pid );

        // so this is distributed across multi-nodes so the lock
        // doesn't fully help but it does help a little bit for multi-proc
        // on this node
    double time_start = Util::getTime();
    Util::MutexLock( &shared.container_mutex );
    int extra_attempts = 0;
    if (shared.createdContainers.find(expanded_path)
            ==shared.createdContainers.end()) 
    {
        res = plfs_create( expanded_path.c_str(), mode, flags );
        self->extra_attempts += extra_attempts;
        if ( res == 0 ) {
            shared.createdContainers.insert( expanded_path );
            self->known_modes[expanded_path] = mode;
        }
    }
    Util::MutexUnlock( &shared.container_mutex );

    double time_end = Util::getTime();
    self->make_container_time += (time_end - time_start);
    if ( time_end - time_start > 2 ) {
        fprintf( stderr, "WTF: %s of %s took %.2f secs\n", __FUNCTION__,
                expanded_path.c_str(), time_end - time_start );
        self->wtfs++;
    }
    return res;
}

// slight chance that the access file doesn't exist yet.
int Plfs::f_access(const char *path, int mask) {
    EXIT_IF_DEBUG;
    PLFS_ENTER;
    ret = plfs_access( strPath.c_str(), int mask );
    PLFS_EXIT;
}

int Plfs::f_mknod(const char *path, mode_t mode, dev_t rdev) {
    PLFS_ENTER;
    ret = makePlfsFile( strPath.c_str(), mode, 0 );
    PLFS_EXIT;
}

// very strange.  When fuse gets the ENOSYS for 
// create, it then calls mknod.  The same exact code which works in
// mknod fails if we put it in here
// maybe that's specific to mac's
// now I'm seeing that *usually* when f_create gets the -ENOSYS, that the
// caller will then call f_mknod, but that doesn't always happen in big
// untar of tarballs, so I'm gonna try to call f_mknod here
int Plfs::f_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    PLFS_ENTER_PID;
    //ret = f_mknod( strPath.c_str(), mode, 0 );
    ret = -ENOSYS;
    PLFS_EXIT;
}

// returns 0 or -errno
// nothing to do for a read file
int Plfs::f_fsync(const char *path, int datasync, struct fuse_file_info *fi) {
    PLFS_ENTER_PID; SETUP_OPEN_FILES;
    if ( wf ) {
        plfs_sync( of );
    }
    PLFS_EXIT;
}

// this means it is an open file.  That means we also need to check our
// current write file and adjust those indices also if necessary
int Plfs::f_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi)
{
    PLFS_ENTER_PID; SETUP_OPEN_FILES;
    ret = plfs_truncate( of, strPath.c_str(), offset );
    PLFS_EXIT;
}

// use removeDirectoryTree to remove all data but not the dir structure
// return 0 or -errno 
int Plfs::f_truncate( const char *path, off_t offset ) {
    PLFS_ENTER;
    ret = plfs_truncate( NULL, strPath.c_str(), offset );
    PLFS_EXIT;
}

int Plfs::f_fgetattr(const char *path, struct stat *stbuf, 
        struct fuse_file_info *fi) 
{
    PLFS_ENTER;
    ret = plfs_getattr( fi->fh, strPath.c_str(), stbuf );
    PLFS_EXIT;
}

int Plfs::f_getattr(const char *path, struct stat *stbuf) {
    PLFS_ENTER;
    ret = plfs_getattr( NULL, strPath.c_str(), stbuf );
    PLFS_EXIT;
}

// a shortcut for functions that are expecting zero
int Plfs::retValue( int res ) {
    return Util::retValue( res );
}

// needs to work differently for directories
int Plfs::f_utime (const char *path, struct utimbuf *ut) {
    PLFS_ENTER;
    if ( isDirectory( strPath ) ) {
        // TODO
    } else {
        ret = plfs_utime( strPath.c_str(), ut );
    }
    PLFS_EXIT;
}
		    
// this needs to recurse on all data and index files
int Plfs::f_chmod (const char *path, mode_t mode) {
    PLFS_ENTER;
    if ( isDirectory( strPath ) ) {
        // this code needs to be replicated for everything that iterates
        // over cloned (i.e. backend) directories
        vector<string>::iterator itr;
        for( itr = shared.params.backends.begin();
             itr!= shared.params.backends.end();
             itr ++ )
        {
            string full( *itr + "/" + path );
            ret = retValue( Util::Chmod( full.c_str(), mode ) );
            if ( ret != 0 ) break;
        }
    } else {
        ret = plfs_chmod( strPath.c_str(), mode );
    }
    if ( ret == 0 ) {
        self->known_modes[strPath] = mode;
    }
    PLFS_EXIT;
}
		    
int Plfs::f_chown (const char *path, uid_t uid, gid_t gid ) { 
    PLFS_ENTER;
    if ( isDirectory( strPath ) ) {
        vector<string>::iterator itr;
        for( itr = shared.params.backends.begin();
             itr!= shared.params.backends.end();
             itr ++ )
        {
            string full( *itr + "/" + path );
            ret = retValue( Util::Chown( full.c_str(), uid, gid ) );
            if ( ret != 0 ) break;
        }
    } else {
        ret = retValue( plfs_chown( strPath.c_str(), uid, gid ) );  
    }
    PLFS_EXIT;
}

// in order to distribute metadata load across multiple MDS, maintain a
// parallel directory structure on all the backends.  Files just go to one
// location but dirs need to be mirrored on all
// probably this should be transactional but it really shouldn't happen
// that we succeed for some and fail for others
int Plfs::f_mkdir (const char *path, mode_t mode ) {
    PLFS_ENTER;
    vector<string>::iterator itr;
    for( itr = shared.params.backends.begin();
         itr!= shared.params.backends.end();
         itr ++ )
    {
        string full( *itr + "/" + path );
        ret = retValue( Util::Rmdir( full.c_str() ) );
        if ( ret != 0 ) break;
    }
    PLFS_EXIT;
}

int Plfs::f_rmdir( const char *path ) {
    PLFS_ENTER;
    // this might show a better way to do it:
    // http://www.gamedev.net/community/forums/topic.asp?topic_id=502800
    int save_ret = 0;
    vector<string>::iterator itr;
    for( itr = shared.params.backends.begin();
         itr!= shared.params.backends.end();
         itr ++ )
    {
        string full( *itr + "/" + path );
        ret = retValue( Util::Rmdir( full.c_str() ) );
        // don't break on an error.  keep trying to delete rest of them
        if ( ret != 0 ) save_ret = ret;
    }
    ret = save_ret;
    PLFS_EXIT;
}

// what if someone is calling unlink on an open file?
// boy I hope that never happens.  Actually, I think this should be OK
// because I believe that f_write will recreate the container if necessary.
// but not sure what will happen on a file open for read.
//
// anyway, not sure we need to worry about handling this weird stuff
// fine to leave it undefined.  users shouldn't do stupid stuff like this anyway
int Plfs::f_unlink( const char *path ) {
    PLFS_ENTER;
    ret = plfs_unlink( path );
    PLFS_EXIT;
}

int Plfs::removeWriteFile( WriteFile *of, string strPath ) {
    int ret = of->Close();  // close any open fds
    delete( of );
    self->write_files.erase( strPath );
    shared.createdContainers.erase( strPath );
    Container::removeOpenrecord( strPath.c_str(), (shared.myhost).c_str() );
    return ret;
}

int Plfs::removeIndex( string path, Index *index ) {
    delete( index );
    shared.read_files.erase( path );
    return 0;
}

// see f_readdir for some documentation here
// returns 0 or -errno
int Plfs::f_opendir( const char *path, struct fuse_file_info *fi ) {
    PLFS_ENTER_PID;
    PLFS_EXIT;
}

// before we had multiple backends, we actually did the opendir in
// opendir, stashed it, used it here, and closed it in closedir
// but now that we have multiple backends, that isn't good enough
// also, much harder to use filler and offset.  so ignore the offset,
// and pass 0 as last arg to filler
int Plfs::f_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		off_t offset, struct fuse_file_info *fi) 
{
    PLFS_ENTER_PID;
    vector<string>::iterator itr;
    set<string> entries;

    for( itr = shared.params.backends.begin();
         itr!= shared.params.backends.end();
         itr++ )
    {
        DIR *dp;
        string dirpath( *itr ); dirpath += "/"; dirpath += path;
        cerr << "Will opendir " << dirpath << endl;
        ret = Util::Opendir( dirpath.c_str(), &dp );
        if ( ret != 0 || ! dp ) {
            break;
        }
        (void) path;
        struct dirent *de;
        while ((de = readdir(dp)) != NULL) {
            //cerr << "Found entry " << de->d_name << endl;
            if( entries.find(de->d_name) != entries.end() ) continue;
            entries.insert( de->d_name );
            struct stat st;
            memset(&st, 0, sizeof(st));
            st.st_ino = de->d_ino;
            st.st_mode = de->d_type << 12;
            string fullPath( dirpath + "/" + de->d_name );
            if ( ! isDirectory( fullPath.c_str() ) ) {
                st.st_mode = Container::fileMode( st.st_mode );
            }
            if (filler(buf, de->d_name, &st, 0)) {
                cerr << "WTF?  filler failed." << endl;
                break;
            }
        }
        Util::Closedir( dp );
    }
    PLFS_EXIT;
}

int Plfs::f_releasedir( const char *path, struct fuse_file_info *fi ) {
    PLFS_ENTER_PID;
    PLFS_EXIT;
}

// checks for access to a file
// returns 0 or -errno
int Plfs::checkAccess( string strPath, struct fuse_file_info *fi ) {
    int ret = 0;
    int accessmask = ( fi->flags & O_WRONLY || fi->flags & O_RDWR 
                   ? W_OK : R_OK );
    string accessfile = Container::getAccessFilePath(strPath);
    ret = Util::Access( accessfile.c_str(), accessmask );
    if ( ret != 0 && errno == ENOENT ) {
            // it's possible that the access file doesn't exist yet
            // if so, just ask about the directory itself.
        ret = Util::Access( strPath.c_str(), accessmask );
    }
    ret = retValue( ret );
    cerr << "Checked access for " << strPath << ": " << ret << endl;
    return ret; 
}

// returns 0 or -errno
// O_WRONLY and O_RDWR are handled as a write
// O_RDONLY is handled as a read
// PLFS is optimized for O_WRONLY and tries to do OK for O_RDONLY
// O_RDWR is optimized for writes but the reads might be horrible
int Plfs::f_open(const char *path, struct fuse_file_info *fi) {
    fi->fh = (uint64_t)NULL;
    EXIT_IF_DEBUG;
    PLFS_ENTER_PID;
    WriteFile *wf    = NULL;
    Index     *index = NULL;

    // make sure we're allowed to open this container
    // this breaks things when tar is trying to create new files
    // with --r--r--r bec we create it w/ that access and then 
    // we can't write to it
    //ret = checkAccess( strPath, fi ); 

    // go ahead and open it here, pass O_CREAT to create it if it doesn't exist 
    // "open it" just means create the data structures we use 
    if ( ret == 0 && (fi->flags & O_WRONLY || fi->flags & O_RDWR) ) {
        bool bufferindex = shared.params.bufferindex;
        if ( fi->flags & O_RDWR ) {
            bufferindex  = false;
            self->o_rdwrs++;
        }
        Util::MutexLock( &shared.fd_mutex );
        wf = getWriteFile( strPath, O_CREAT, bufferindex );
        wf->incrementOpens( 1 );
        Util::MutexUnlock( &shared.fd_mutex );
    } else if ( ret == 0 ) {
        Util::MutexLock( &shared.index_mutex );
        ret = getIndex( strPath, O_CREAT, &index ); 
        if ( ret != 0 ) {
            index = NULL;
            fprintf( stderr, "WTF.  getIndex for %s failed\n", strPath.c_str());
            self->wtfs++;
        } else {
            index->incrementOpens( 1 );
        }
        Util::MutexUnlock( &shared.index_mutex );
    }

    if ( ret == 0 ) {
        fi->fh = (uint64_t)new PlfsFd( wf, index, fuse_get_context()->pid ); 
    }
    PLFS_EXIT;
}

// the release happens when all pids on a machine close the file
// but it happens multiple times (one for each proc who had it open)
// because multiple pids on a machine will open a file, we should
// make a structure that is keyed by the logical path which then
// contains fds for all the physical paths, then on release, we
// clean them all up, 
// probably we shouldn't see any IO after the first release
// so it should be safe to close all fd's on the first release
// and delete it.  But it's safer to wait until the last release
int Plfs::f_release( const char *path, struct fuse_file_info *fi ) {
    PLFS_ENTER_PID; SETUP_OPEN_FILES;

    if ( wf ) {
        off_t  last_offset;
        size_t total_bytes;
        of->getMeta( &last_offset, &total_bytes );
        ret = plfs_sync( of );  // make sure all data is sync'd
        Util::MutexLock( &shared.fd_mutex );
            // only release it on the final release
        wf->addMeta( last_offset, total_bytes );
        int remaining_opens = wf->incrementOpens( -1 );
        if ( remaining_opens == 0 ) {
            // before we remove it, let's save some metadata
            wf->getMeta( &last_offset, &total_bytes );
            Container::addMeta( last_offset, total_bytes, 
                    strPath.c_str(), (shared.myhost).c_str() );
            ret = removeWriteFile( wf, strPath );
        }
        Util::MutexUnlock( &shared.fd_mutex );
    } else if ( index ) {
        Util::MutexLock( &shared.index_mutex );
        int remaining_opens = index->incrementOpens( -1 );
        if ( remaining_opens == 0 ) {
            ret = removeIndex( strPath, index );
        }
        Util::MutexUnlock( &shared.index_mutex );
    }

    delete( of );
    PLFS_EXIT;
}

// look for an instantiation of an WriteFile
// if mode & O_CREAT, create one and cache it if not exist
// when this is called, we should already be in a mux 
WriteFile *Plfs::getWriteFile( string expanded, mode_t mode, bool bufferindex ){
    WriteFile *of = NULL;
    fprintf( stderr, "Looking for WriteFile for %s\n", expanded.c_str() );
    HASH_MAP<string, WriteFile *>::iterator itr;
    itr = self->write_files.find( expanded );
    if ( itr == self->write_files.end() ) {
        if ( mode & O_CREAT ) {
                // we should get the mode from when the container was created
                // into the writefile so new index files and data files are
                // created appropriately
            mode_t wf_mode =getMode( expanded );
            of = new WriteFile( expanded, shared.myhost, bufferindex, wf_mode );
            self->write_files[expanded] = of;
            Container::addOpenrecord(expanded.c_str(),(shared.myhost).c_str());
        }
    } else {
        of = itr->second;
    }
    return of;
}

mode_t Plfs::getMode( string expanded ) {
    mode_t mode;
    HASH_MAP<string, mode_t>::iterator itr =
            self->known_modes.find( expanded );
    if ( itr == self->known_modes.end() ) {
        mode = Container::getmode( expanded.c_str() );
        self->known_modes[expanded] = mode;
    } else {
        mode = itr->second; 
    }
    return mode;
}
            
// should be in a mutex
int Plfs::getIndex( string expanded, mode_t mode, Index **index ) {
    fprintf( stderr, "Looking for Index for %s\n", expanded.c_str() );
    HASH_MAP<string, Index *>::iterator itr;
    itr = shared.read_files.find( expanded );
    if ( itr == shared.read_files.end() ) {
        if ( mode & O_CREAT ) {
            *index = new Index( expanded, SHARED_PID );
            int ret = Container::populateIndex( expanded.c_str(), *index );
            if ( ret != 0 ) {
                    // no existing found and user asked to create one but
                    // something weird happened while reading index files
                delete( *index );
                *index = NULL;
                return -EIO;
            } else {
                    // no existing found so 
                    // we created and populated a new one
                shared.read_files[expanded] = *index;
                return 0;
            }
        } else {
                // no index found and user didn't ask to make one
            *index = NULL;
            return -ENOENT;
        }
    } else {
            // existing index found
        *index = itr->second;
        return 0;
    }
}

// here we lock two muxes, first fd, then container (container just in case)
// returns 0 or -errno
int Plfs::getWriteFds( string strPath, int *data_fd, int *indx_fd, 
        Index **index, PlfsFd *of ) 
{
    int ret = 0;
    of->getWriteFds( data_fd, indx_fd, index );
    if ( *data_fd < 0 || indx_fd < 0 ) {
        WriteFile *wf = of->getWritefile();
        Util::MutexLock( &shared.fd_mutex );
        for( int attempts = 0; attempts <= 1 && ret == 0; attempts++ ) {
            ret=wf->getFds( of->getPid(), O_CREAT, indx_fd, data_fd, index );
            if ( ret == -ENOENT && attempts == 0 ) {
                // we might have to make a host dir if one doesn't exist yet
                double time_start = Util::getTime();
                self->extra_attempts++;
                cerr << "Making hostdir for " << strPath << endl;
                Util::MutexLock( &shared.container_mutex );
                // should probably check in here for whether already made
                if (shared.createdContainers.find(strPath)
                        ==shared.createdContainers.end()) 
                {
                    ret = Container::makeHostDir( strPath.c_str(), 
                        shared.myhost.c_str(), getMode(strPath) );
                    if ( ret == 0 ) {
                        shared.createdContainers.insert( strPath );
                    }
                }
                Util::MutexUnlock( &shared.container_mutex );
                double time_end = Util::getTime();
                self->make_container_time += (time_end - time_start);
            } else {
                break;
            }
        }
        Util::MutexUnlock( &shared.fd_mutex );
        if ( ret == 0 ) {
            of->setWriteFds( *data_fd, *indx_fd, *index );
        }
    }
    return ret;
}


int Plfs::f_write(const char *path, const char *buf, size_t size, off_t offset,
		struct fuse_file_info *fi) 
{
    PLFS_ENTER_IO; SETUP_OPEN_FILES;
    cerr << __FUNCTION__ << " on " << path << ", off " << offset << ", len "
         << size << endl;
    int data_fd, indx_fd;

    // get the fds and the index we need.  they'll be created if necessary 
    ret = getWriteFds( strPath, &data_fd, &indx_fd, &index, of );

        // write the bytes to the data file
        // use Util::Writen (?)
        // hmm.  for some reason, 131072 writes often only write 131064...
        // this is because fuse writes need to be block aligned.
        // so app using valloc might help.
        // wow.  Writen seems to make it go much slower....
        // not sure why...
    if ( ret == 0 ) {
        double begin_timestamp = 0, end_timestamp = 0;
        #ifdef INDEX_CONTAINS_TIMESTAMPS
            begin_timestamp = Util::getTime();
        #endif
        ret = Util::Write( data_fd, buf, size );
        #ifdef INDEX_CONTAINS_TIMESTAMPS
            end_timestamp = Util::getTime();
        #endif
        if ( ret < 0 ) {
            ret = -errno;
        }
        size_t written = ret;
	#ifdef COUNT_SKIPS
        Util::MutexLock( &shared.index_mutex);
	    HASH_MAP<int, int>::iterator itr;
	    itr = self->last_offsets.find( data_fd );
	    if ( itr == self->last_offsets.end() ) {
	      self->nonskip_writes++; // The first write to a datafile never skips
	    } else {
	      int last_off = itr->second;
	      if ( offset < last_off ) {
		self->bward_skips++;
	      } else if ( offset > last_off ) {
		self->fward_skips++;
	      } else {
		// this write is sequential to the last
		self->nonskip_writes++;
	      }
	    }
	    self->last_offsets[data_fd] = offset+size;
        Util::MutexUnlock( &shared.index_mutex );
	#endif 
            // record the write even if we don't actually buffer the index
            // so that we can remember it for flush and release so we can
            // write out meta data to speed up stat
        if ( ret >= 0 ) {
            of->addWrite( offset, written );
                // either buffer it or write it directly
            cerr << "Adding write with " << begin_timestamp 
                 << " and " << end_timestamp << endl;
            if ( index ) {
                    //  no lock required here bec this index is not shared
                index->addWrite( offset, written, 
                        begin_timestamp, end_timestamp );
            } else {
                Util::MutexLock( &shared.index_mutex );
                ret = Index::writeIndex( indx_fd, offset, written, 
                        of->getPid(), begin_timestamp, end_timestamp );
                Util::MutexUnlock( &shared.index_mutex );
            }
            if ( ret >= 0 ) {
                ret = written; 
            }
        }

    }
    PLFS_EXIT_IO;
}

int Plfs::f_readlink (const char *path, char *buf, size_t bufsize) {
    PLFS_ENTER;
    ssize_t char_count;
    memset( (void*)buf, 0, bufsize);
    char_count = readlink( strPath.c_str(), buf, bufsize );
    if ( char_count != -1 ) {
        ret = 0;
        cerr << "Readlink at " << strPath.c_str() << ": " << char_count << endl;
    } else {
        ret = retValue( -1 );
    }
    PLFS_EXIT;
}

int Plfs::f_link( const char *path1, const char *path ) {
    PLFS_ENTER;
    cerr << "Making hard link from " << path1 << " to " << strPath << endl;
    cerr << "How do I check for EXDEV here?" << endl;
    ret = retValue( link( path1, strPath.c_str() ) );
    PLFS_EXIT;
}

int Plfs::f_symlink( const char *path1, const char *path ) {
    PLFS_ENTER;
    cerr << "Making symlink from " << path1 << " to " << strPath << endl;
    ret = retValue( Util::Symlink( path1, strPath.c_str() ) );
    PLFS_EXIT;
}

int Plfs::f_statfs(const char *path, struct statvfs *stbuf) {
    PLFS_ENTER;
    // tempting to stick some identifying info in here that we could
    // then pull into our test results like a version or some string
    // identifying any optimizations we're trying.  but the statvfs struct
    // doesn't have anything good.  very sparse.  it does have an f_fsid flag.
    ret = retValue( statvfs( strPath.c_str(), stbuf ) );
    PLFS_EXIT;
}

// returns bytes read or -errno
int Plfs::f_readn(const char *path, char *buf, size_t size, off_t offset,
		struct fuse_file_info *fi) 
{
    if ( isdebugfile( path ) ) {
        return writeDebug( buf, size, offset, path );
    }

    PLFS_ENTER_IO; SETUP_OPEN_FILES;

    // possible that we opened the file as O_RDWR
    // if so, build an index now, but destroy it after this IO
    // so that new writes are re-indexed for new reads
    // basically O_RDWR is possible but it can reduce read BW
    bool new_index_created = false;  

    if ( index == NULL ) {
        // need to lock this since it's a shared structure
        // which means they are sharing the fds as well
        Util::MutexLock( &shared.index_mutex );
        ret = getIndex( strPath, O_CREAT, &index );
        if ( ret != 0 ) {
            fprintf( stderr, "WTF.  getIndex for %s failed\n", strPath.c_str());
            self->wtfs++;
        } else {
            new_index_created = true;
            index->incrementOpens( 1 );
        }
        Util::MutexUnlock( &shared.index_mutex );
    }

    // actually do the reads now that we have an index
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
            if ( ret > 0 && bytes_remaining ) {
                cerr << "Reading multiple chunks from " << strPath << endl;
            }
        } while( bytes_remaining && ret > 0 );
        ret = ( ret < 0 ? ret : bytes_read );
    }
        // actually bytes_remaining won't go to 0 when read goes beyond EOF 
        //assert( bytes_remaining == 0 );

    if ( new_index_created ) {
        Util::MutexLock( &shared.index_mutex );
        if ( index->incrementOpens( -1 ) <= 0 ) {
            removeIndex( strPath, index );
        }
        Util::MutexUnlock( &shared.index_mutex );
    }
    PLFS_EXIT_IO;
}

// returns bytes read or -errno
// we may already be holding the mutex.  check here.
int Plfs::read_helper( Index *index, char *buf, size_t size, off_t offset ) {
    off_t  chunk_offset = 0;
    size_t chunk_length = 0;
    int    fd           = -1;

    // need to lock this since the globalLookup does the open of the fds
    Util::MutexLock( &shared.index_mutex );
    int ret = index->globalLookup( &fd, &chunk_offset, &chunk_length, offset );
    Util::MutexUnlock( &shared.index_mutex );
    if ( ret != 0 ) return ret;

    size_t read_size = ( size < chunk_length ? size : chunk_length );
    if ( read_size > 0 ) {
        // use pread since the fd's are shared  
        if ( fd >= 0 ) {
            cerr << dec << "Reading " << read_size << " from " << fd << endl;
            ret = Util::Pread( fd, buf, read_size, chunk_offset );
            if ( ret < 0 ) {
                cerr << "Couldn't read in " << fd << ":" << strerror(errno) << endl;
                return -errno;
            }
        } else {
            cerr << dec << "Zero filling hole of " << read_size << endl;
            memset( (void*)buf, 0, read_size);
            ret = read_size;
        }
    } else {
        // when chunk_length = 0, that means we're at EOF
        ret = 0;
    }
    return ret;
}

string Plfs::writeFilesToString() {
    ostringstream oss;
    int quant = self->write_files.size();
    oss << quant << " WriteFiles" << ( quant ? ": " : "" );
    HASH_MAP<string, WriteFile *>::iterator itr;
    for(itr = self->write_files.begin(); itr != self->write_files.end(); itr++){
        oss << itr->first << " ";
    }
    oss << "\n";
    return oss.str();
}

string Plfs::readFilesToString() {
    ostringstream oss;
    int quant = shared.read_files.size(); 
    oss << quant << " ReadFiles" << ( quant ? ": " : "" );
    HASH_MAP<string, Index *>::iterator itr;
    for(itr = shared.read_files.begin(); itr != shared.read_files.end(); itr++){
        oss << itr->first << " ";
    }
    oss << "\n";
    return oss.str();
}

int Plfs::writeDebug( char *buf, size_t size, off_t offset, const char *path ) {

        // make sure we don't allow them to read more than we have
    size_t validsize; 
    if ( size + offset > DEBUGFILESIZE ) {
        if ( DEBUGFILESIZE > offset ) {
            validsize = DEBUGFILESIZE - offset;
        } else {
            validsize = 0;
        }
    } else {
        validsize = size;
    }

    char *tmpbuf = (char*)malloc( sizeof(char) * DEBUGFILESIZE );
    if ( ! tmpbuf ) {
        cerr << "WTF?  Malloc failed: " << strerror(errno) << endl;
        return 0;
    }
    int  ret;
    memset( buf, 0, size );
    memset( tmpbuf, 0, DEBUGFILESIZE );

    if ( isdebugfile( path, DEBUGFILE ) ) {
        ret = snprintf( tmpbuf, DEBUGFILESIZE, 
                "Hostname %s, %.2f Uptime\n"
                "%s"
                "%s"
                "%.2f MakeContainerTime\n"
                "%d WTFs\n"
                "%d ExtraAttempts\n"
                "%d Opens with O_RDWR\n"
		#ifdef COUNT_SKIPS
                "%d forward skips in datafiles\n"
                "%d backward skips in datafiles\n"
                "%d sequential writes to datafiles\n"
        #endif 
                "%s"
                "%s",
                shared.myhost.c_str(), 
                Util::getTime() - self->begin_time,
                paramsToString(&(shared.params)).c_str(),
                Util::toString().c_str(),
                self->make_container_time,
                self->wtfs,
                self->extra_attempts,
                self->o_rdwrs,
		#ifdef COUNT_SKIPS
                self->fward_skips,
                self->bward_skips,
                self->nonskip_writes,
		#endif
                writeFilesToString().c_str(),
                readFilesToString().c_str() );
    } else {
        ret = snprintf(tmpbuf, DEBUGFILESIZE, "%s", LogMessage::Dump().c_str());
    }
    if ( ret >= DEBUGFILESIZE ) {
        LogMessage lm;
        lm << "WARNING:  DEBUGFILESIZE is too small" << endl;
        lm.flush();
    }
    memcpy( buf, (const void*)&(tmpbuf[offset]), validsize );
    free( tmpbuf );
    return validsize; 
}

string Plfs::paramsToString( Params *p ) {
    ostringstream oss;
    oss << "BufferIndex: "      << p->bufferindex << endl
        << "ContainerSubdirs: " << p->subdirs     << endl
        << "SyncOnClose: "      << p->sync_on_close     << endl
        << "Backends: "
        ;
    vector<string>::iterator itr;
    for ( itr = p->backends.begin(); itr != p->backends.end(); itr++ ) {
        oss << *itr << ",";
    }
    oss << endl;
    return oss.str();
}

// this should be called when an app does a close on a file
// 
// for RDONLY files, nothing happens here, they should get cleaned
// up in the f_release
// 
// actually, we don't want to close here because sometimes this can
// be called before some IO's.  So for safety sake, we should sync
// here and do the close in the release
int Plfs::f_flush( const char *path, struct fuse_file_info *fi ) {
    PLFS_ENTER_IO; SETUP_OPEN_FILES;
    if ( wf ) {
            // we could change this here to:
            // 1) close the datafd
            // 2) sync the datafd and maybe the indexfd
            // 3) do nothing and let f_release do it
        bool sync_index = true;
        ret = plfs_sync( of );
    }
    PLFS_EXIT_IO;
}

// returns 0 or -errno
int Plfs::f_rename( const char *path, const char *to ) {
    PLFS_ENTER;
    string toPath   = expandPath( to );

    // we can't just call rename bec it won't trash a dir in the toPath
    // this might fail with ENOENT but that's fine
    plfs_unlink( toPath.c_str() );

    ret = retValue( Util::Rename( strPath.c_str(), toPath.c_str() ) );
    PLFS_EXIT;
}
