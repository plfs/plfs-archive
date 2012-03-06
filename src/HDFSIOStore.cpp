#include <errno.h>
#include <iostream>
#include <sys/types.h>
#include <sys/wait.h>
#include <grp.h>
#include <pwd.h>
#include <string.h>
#include "Util.h"
#include "HDFSIOStore.h"

/*
 * copy of TAILQ.  not all system have <sys/queue.h> so just replicate
 * a bit of it here so that we know we've got it covered.
 */
#define _XTAILQ_HEAD(name, type, qual)                                  \
struct name {                                                           \
        qual type *tqh_first;           /* first element */             \
        qual type *qual *tqh_last;      /* addr of last next element */ \
}       
#define XTAILQ_HEAD(name, type)  _XTAILQ_HEAD(name, struct type,)
 
#define XTAILQ_HEAD_INITIALIZER(head)                                   \
        { NULL, &(head).tqh_first }

#define _XTAILQ_ENTRY(type, qual)                                      \
struct {                                                                \
        qual type *tqe_next;            /* next element */              \
        qual type *qual *tqe_prev;      /* address of previous next element */\
}
#define XTAILQ_ENTRY(type)       _XTAILQ_ENTRY(struct type,)

#define XTAILQ_INIT(head) do {                                          \
        (head)->tqh_first = NULL;                                       \
        (head)->tqh_last = &(head)->tqh_first;                          \
} while (/*CONSTCOND*/0)

#define XTAILQ_EMPTY(head)               ((head)->tqh_first == NULL)
#define XTAILQ_FIRST(head)               ((head)->tqh_first)

#define XTAILQ_REMOVE(head, elm, field) do {                            \
        if (((elm)->field.tqe_next) != NULL)                            \
                (elm)->field.tqe_next->field.tqe_prev =                 \
                    (elm)->field.tqe_prev;                              \
        else                                                            \
                (head)->tqh_last = (elm)->field.tqe_prev;               \
        *(elm)->field.tqe_prev = (elm)->field.tqe_next;                 \
} while (/*CONSTCOND*/0)

#define XTAILQ_INSERT_HEAD(head, elm, field) do {                       \
        if (((elm)->field.tqe_next = (head)->tqh_first) != NULL)        \
                (head)->tqh_first->field.tqe_prev =                     \
                    &(elm)->field.tqe_next;                             \
        else                                                            \
                (head)->tqh_last = &(elm)->field.tqe_next;              \
        (head)->tqh_first = (elm);                                      \
        (elm)->field.tqe_prev = &(head)->tqh_first;                     \
} while (/*CONSTCOND*/0)

/*
 * XXXCDC: hdfs API wrapper code.
 *
 * There is a bug/issue with HDFS/Java and FUSE that causes it to leak
 * memory (espeically during big file reads).  FUSE appears to create
 * a new pthread for each request it receives --- that thread handles
 * the request and then exits.  Having each HDFS request come from a
 * different thread confuses HDFS/Java and it leaks memory.   I have a
 * standalone test program (readkill.c) clearly shows this.
 *
 * The work around for this is to have a fixed pool of thread handle
 * almost all HDFS API.  That way only a small number of pthreads make
 * HDFS/Java API calls.
 *
 * To make this work, we define stub wrapper functions for all HDFS API
 * calls.  The stubs handle the args and call hdfs_execute() to allocate
 * a thread from the pool to process the request.
 *
 * The setup for this is to define all the HDFS APIs in "HDFS.api" and
 * then run the perl script "hdfswrap.pl" to generate the "hdfswrap.h"
 * file.  This file contains 3 items for each API:
 *   1. an "_args" structure to collect args and ret vals
 *   2. a "_handler" callback that executes in the thread pool and calls
 *      the real HDFS API into Java
 *   3. a "_wrap" wrapper function that loads an _args structure and
 *      calls hdfs_execute() to send the API request to a thread in
 *      the thread pool.
 *
 * at run time:
 *   1. a FUSE thread calls the "_wrap" function it want to use
 *   2. the "_wrap" function loads the "_args" struct and calls hdfs_execute()
 *   3. hdfs_execute():
 *       - ensures we are connected to the HDFS
 *       - allocates a thread from the pool, creating a new one if appropriate
 *       - if the thread pool is full, then it waits for a thread to free
 *       - the allocated thread is loaded with the request and dispatched
 *       - we block waiting for the request to be serviced
 *       - when the request is done we collect results and free the thread
 *   4. the "_wrap" function returns the results to the caller
 *
 * note that hdfs_execute() works around another bug as well... if we
 * connect to a HDFS filesystem and then fork, the forked child will
 * not be able to make calls into HDFS (java) --- they will just hang.
 * so we make the main connection to HDFS happen in hdfs_execute() so that
 * FUSE daemon processes will not have their requests hang.  (the old
 * version would connect to HDFS at init time, then fork a daemon process,
 * expecting that daemon to be able to use the HDFS connection... that
 * doesn't work).
 *
 * The thread pool stuff should work for non-FUSE-based PLFS apps as
 * well.   It isn't really necessary in non-FUSE cases, but it doesn't
 * hurt to have it here anyway (other than a little additional overhead).
 */

#include "hdfswrap.h"
/*
 * define a limit to the total number of threads than can be in our
 * HDFS I/O pool.   hopefully this is larger than anything FUSE will
 * need.
 */
#define HDFS_MAXTHR 64   /* max number of thread */
#define HDFS_PRETHR 64   /* number to preallocate at boot */

/**
 * hdfs_opthread: describes a thread in the HDFS I/O pool
 */
struct hdfs_opthread {
    /* the 3 args from hdfs_execute() are stored in the next 3 fields... */
    const char *funame;    /*!< just for debugging */
    void *fuarg;           /*!< arg for handler function */
    void (*fuhand)(void *ap);       /*!< the "_handler" function to call */
    
    pthread_t worker;      /*!< id of this worker thread (for debug) */
    int myerrno;           /*!< passed back up to wrapper */
    int done;              /*!< set to 1 when thread's work is complete */
    pthread_cond_t hold;   /*!< opthread holds here waiting for work */
    pthread_cond_t block;  /*!< caller blocks here for reply */
    XTAILQ_ENTRY(hdfs_opthread) q;  /*!< busy/free linkage */
};

/**
 * hdfs_opq: a queue of threads (busy/free)
 */
XTAILQ_HEAD(hdfs_opq, hdfs_opthread);

/**
 * hdfs_gtstate: HDFS global thread pool state
 */
struct hdfs_gtstate {
    pthread_mutex_t gts_mux; /*!< protects all global state */
    hdfsFS *fsp;             /*!< points to iostore object fs field */
    const char *hname;       /*!< for reconnect */
    int port;                /*!< for reconnect */
    struct hdfs_opq free;    /*!< queue of free threads */
    struct hdfs_opq busy;    /*!< queue of busy threads */
    int nthread;             /*!< total number of threads allocated */
    int nwanted;             /*!< non-zero we are waiting for a free thread */
    pthread_cond_t thblk;    /*!< block here if waiting for a free thread */
};

static struct hdfs_gtstate hst = { 0 };   /* global state here! */

/**
 * hdfs_opr_main: main routine for operator thread
 *
 * @param args points to this thread's opthread structure
 */
void *hdfs_opr_main(void *args) {
    struct hdfs_opthread *me;

    me = (struct hdfs_opthread *)args;

    /*
     * bootstrap task: the thread that created us is waiting on
     * me->block for us to finish our init.
     */
    pthread_mutex_lock(&hst.gts_mux);
    XTAILQ_INSERT_HEAD(&hst.free, me, q);
    me->done = 1;
    pthread_cond_signal(&me->block);
    
    while (1) {
        /* drops lock and wait for work */
        pthread_cond_wait(&me->hold, &hst.gts_mux);
        if (me->done)
            continue;    /* wasn't for me */

        /* drop lock while we are talking to HDFS */
        pthread_mutex_unlock(&hst.gts_mux);
        me->fuhand(me->fuarg);   /* do the callback */
        me->myerrno = errno;     /* save errno */
        pthread_mutex_lock(&hst.gts_mux);

        /* mark our job as done and signal caller (waiting in hdfs_execute) */
        me->done = 1;
        pthread_cond_signal(&me->block);
    }

    /*NOTREACHED*/
    pthread_mutex_unlock(&hst.gts_mux);
    return(NULL);
}

/**
 * hdfs_alloc_opthread: allocate a new opthread
 *
 * @return the new opthread structure
 */
struct hdfs_opthread *hdfs_alloc_opthread() {
    struct hdfs_opthread *opr;
    static int attr_init = 0;
    static pthread_attr_t attr;
    int chold, cblock;

    /* one time static init */
    if (attr_init == 0) {
        if (pthread_attr_init(&attr) != 0) {
            fprintf(stderr, "hdfs_alloc_opthread: attr init failed?!\n");
            return(NULL);
        }
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
        attr_init++;
    }

    opr = (struct hdfs_opthread *)malloc(sizeof(*opr));
    if (!opr)
        return(NULL);
    opr->done = 0;

    chold = pthread_cond_init(&opr->hold, NULL);
    cblock = pthread_cond_init(&opr->block, NULL);
    if (pthread_create(&opr->worker, &attr, hdfs_opr_main, opr) != 0) {
        fprintf(stderr, "pthread_create failed!\n");
        goto failed;
    }
    return(opr);
    /*NOTREACHED*/

 failed:
    if (chold == 0) pthread_cond_destroy(&opr->hold);
    if (cblock == 0) pthread_cond_destroy(&opr->block);
    free(opr);
    fprintf(stderr, "hdfs_alloc_opthread: FAILED\n");
    return(NULL);
}

/**
 * hdfs_execute: run a HDFS function inside our HDFS thread pool
 *
 * @param name the name of the function we are calling (for debug)
 * @param a the args to the handler function
 * @param hand pointer to the handler function
 */
void hdfs_execute(const char *name, void *a, void (*hand)(void *ap)) {
    struct hdfs_opthread *opr;
    int lcv, mustwait;

    pthread_mutex_lock(&hst.gts_mux);
    
    /* re-establish connection to HDFS if necessary */
    if (*hst.fsp == NULL) {
        *hst.fsp = hdfsConnect(hst.hname, hst.port);  /* don't wrap this */
        if (*hst.fsp == NULL) {
            fprintf(stderr, "hdfs_execute: reestablish failed!\n");
            /* XXX: what else can we do but exit? */
            exit(1);
        }

        /* preallocate thread pool if requested */
        for (lcv = 0 ; lcv < HDFS_PRETHR ; lcv++) {
            opr = hdfs_alloc_opthread();
            if (opr) {
                hst.nthread++;
                while (opr->done == 0) {
                    pthread_cond_wait(&opr->block, &hst.gts_mux);
                }
            }
        }   /* end of preallocation loop */
      
    }

    /* get an operator thread */
    mustwait = 0;
    while ((opr = XTAILQ_FIRST(&hst.free)) == NULL) {
        /* wait if we must or we are at max threads */
        if (mustwait || hst.nthread >= HDFS_MAXTHR) {
            hst.nwanted++;
            pthread_cond_wait(&hst.thblk, &hst.gts_mux);
            hst.nwanted--;
            continue;    /* try again */
        }

        /* try and allocate a new thread */
        opr = hdfs_alloc_opthread();
        if (opr == NULL) {
            if (hst.nthread == 0) {  /* can't wait for nothing */
                fprintf(stderr, "hdfs_execute: thread alloc deadlock!\n");
                exit(1);
            }
            mustwait = 1;
            continue;
        }

        /* wait for new thread to startup and add to free list... */
        hst.nthread++;
        while (opr->done == 0) {
            pthread_cond_wait(&opr->block, &hst.gts_mux);
        }
        /* try again */
        continue;
    }
    
    /* dispatch thread */
    XTAILQ_REMOVE(&hst.free, opr, q);
    opr->funame = name;
    opr->fuarg = a;
    opr->fuhand = hand;
    opr->myerrno = 0;
    opr->done = 0;
    XTAILQ_INSERT_HEAD(&hst.busy, opr, q);
    pthread_cond_broadcast(&opr->hold);
    
    while (opr->done == 0) {   /* wait for op to complete */
        pthread_cond_wait(&opr->block, &hst.gts_mux);
    }
    
    errno = opr->myerrno;
    XTAILQ_REMOVE(&hst.busy, opr, q);
    XTAILQ_INSERT_HEAD(&hst.free, opr, q);
    if (hst.nwanted > 0)
        pthread_cond_signal(&hst.thblk);
    
    pthread_mutex_unlock(&hst.gts_mux);
}

/**
 * hdfsOpenFile_retry: wrapper for hdfsOpenFile() that retries on failure.
 * XXX: this is temporary until we can figure out the failures i'm getting.
 *
 * @param fs the HDFS filesystem we are using
 * @param path the path to the file to open
 * @param flags open flags
 * @param bufferSize buffer size to use (0==default)
 * @param replication replication to use (0==default)
 * @param blocksize blocksize to use (0==default)
 * @return handle to the open file
 */
static hdfsFile hdfsOpenFile_retry(hdfsFS fs, const char* path, int flags,
                                   int bufferSize, short replication, 
                                   tSize blocksize) {
    hdfsFile file;
    int tries;
 
    /* XXXCDC: could hardwire replication to 1 here */
    
    for (tries = 0, file = NULL ; !file && tries < 5 ; tries++) {
        file = hdfsOpenFile_wrap(fs, path, flags, bufferSize, 
                                 replication, blocksize);
        if (!file) {
            fprintf(stderr, "hdfsOpenFile_retry(%s) failed try %d\n", 
                    path, tries);
        }
    }
    return(file);
}

/**
 * Constructor. Initializes some objects and attempts to connect to the
 * HDFS Filesystem.
 * We start the fd count at a value which is without special meaning.
 */
HDFSIOStore::HDFSIOStore(const char* host, int port)
    : fdMap(), hostName(host), portNum(port), fd_count(3)
{
    pid_t child;
    int status;
    
    /*
     * XXX
     * 
     * we probe HDFS in a child process to avoid JVM issues (FUSE
     * forks a daemon after this call, and the daemon's JVM calls
     * all hang because the JVM was inited in the parent process).
     * the fork avoids this issue.
     */
    child = fork();
    if (child == 0) {
        fs = hdfsConnect(hostName, portNum);  /* don't wrap this */
        hdfsDisconnect(fs);
        exit((fs == NULL) ? 1 : 0);
    }
    status = -1;
    if (child != -1) 
        (void)waitpid(child, &status, 0);
    if (status != 0) {
        fprintf(stderr, "Cannot connect to HDFS(%s,%d)!\n", host, port);
        exit(1);
    }
    fs = NULL;   /* we'll reconnect in hdfs_execute() */
    
    /* Initialize our mutex for protecting the fdMap and fd count. */
    status = pthread_mutex_init(&fd_mc_mutex, NULL);

    /* init the hst */
    status += pthread_mutex_init(&hst.gts_mux, NULL);
    hst.fsp = &fs;
    hst.hname = hostName;
    hst.port = portNum;
    XTAILQ_INIT(&hst.free);
    XTAILQ_INIT(&hst.busy);
    hst.nthread = hst.nwanted = 0;
    status += pthread_cond_init(&hst.thblk, NULL);

    if (status != 0) {
        fprintf(stderr, "HDFS pthread init failed! (%d)\n", status);
        exit(1);
    }
}

/**
 * Adds the specified file to the fdMap, usually on open.
 * it is removed on close.
 * Returns the new fd on success, -1 on error.
 */
int HDFSIOStore::AddFileToMap(hdfsFile file, int open_mode)
{
    int fd;
    struct openFile of;
    of.file = file;
    of.open_mode = open_mode;
    pthread_mutex_lock(&fd_mc_mutex);
    fd = fd_count++;
    fdMap[fd] = of;
    pthread_mutex_unlock(&fd_mc_mutex);

    return fd;
}

/**
 * Removes the specified fd from the Map.
 */
void HDFSIOStore::RemoveFileFromMap(int fd)
{
    pthread_mutex_lock(&fd_mc_mutex);
    fdMap.erase(fd);
    pthread_mutex_unlock(&fd_mc_mutex);
}

/** 
 * Translates a integral fd to the opaque hdfsFile object.
 * Returns null if it's not present.
 */
hdfsFile HDFSIOStore::GetFileFromMap(int fd)
{
    hdfsFile rv;
    map<int, openFile>::iterator it;

    pthread_mutex_lock(&fd_mc_mutex);
    it= fdMap.find(fd);
    if (it == fdMap.end()) {
        rv = NULL;
    } else {
        rv = it->second.file;
    }
    pthread_mutex_unlock(&fd_mc_mutex);

    return(rv);
}

int HDFSIOStore::GetModeFromMap(int fd)
{
    int rv;
    map<int, openFile>::iterator it;

    pthread_mutex_lock(&fd_mc_mutex);
    it= fdMap.find(fd);
    if (it == fdMap.end()) {
        rv = -1;
    } else {
        rv = it->second.open_mode;
    }
    pthread_mutex_unlock(&fd_mc_mutex);

    return(rv);
}

/**
 * Access. For now we assume that we're the owner of the file!
 * This should be changed. So the only thing we really check is 
 * existence.
 */
int HDFSIOStore::Access(const char* path, int mode) 
{
    // hdfsExists says it returns 0 on success, but I believe this to be wrong documentation.
    if ((mode & F_OK) && hdfsExists_wrap(fs, path)) {
        errno = ENOENT;
        return -1;
    }
    return 0;
}

int HDFSIOStore::Chmod(const char* path, mode_t mode) 
{
    return 0; //hdfsChmod_wrap(fs, path, mode);
}

/**
 * Chown. HDFS likes strings, not constants. So we translate these.
 * And hope that the same name exists in the Hadoop DB
 */
int HDFSIOStore::Chown(const char *path, uid_t owner, gid_t group) 
{
    struct group* group_s = getgrgid(group);
    struct passwd* passwd_s = getpwuid(owner);

    if (!group_s || !passwd_s)
        return -1;

    return hdfsChown_wrap(fs, path, passwd_s->pw_name, group_s->gr_name);
}

/**
 * Close. Simple: Look up the hdfsFile in the map, close it if it exists,
 * and remove it from the map.
 */
int HDFSIOStore::Close(int fd) 
{
    int ret;
    hdfsFile openFile;

    openFile = GetFileFromMap(fd);
    //std::cout << "Closing " << fd << "\n";
    if (!openFile) {
        //std::cout << "Error looking it up in map.\n";
        errno = ENOENT;
        return -1;
    }
    /*    std::cout << "FD " << fd << " corresponds to hdfsFile " << openFile << "\n";*/
    //if (GetModeFromMap(fd) == O_WRONLY) {
    if (hdfsFlush_wrap(fs, openFile)) {
        Util::Debug("Couldn't flush file. Probably open for reads.\n");
    }
    //}
    ret = hdfsCloseFile_wrap(fs, openFile);
    if (ret) {
        //std::cout << "Error closing hdfsFile\n";
        return ret;
    }

    /*    // DEBUGGING: Try to re-open the file to debug close error.
    open_path = GetPathFromMap(fd);
    std::cout << "Re-opening " << open_path << "\n";
    openFile = hdfsOpenFile_retry(fs, open_path->c_str(), O_RDONLY,0,0,0);
    if (!openFile) {
        std::cout << "Error re-opening the file!" << errno << "\n";
    } else {
        std::cout << "Could successfully re-open. Interesting....\n";
        hdfsCloseFile_wrap(fs, openFile);
    }
    */
    RemoveFileFromMap(fd);
    return ret;
}

/**
 * On open, we create a struct openDir. Here, on close, we delete the fields and
 * deallocate the structure.
 */
int HDFSIOStore::Closedir(DIR* dirp)
{
    struct openDir *theDir = (struct openDir*)dirp;
    if (theDir->infos)
        hdfsFreeFileInfo_wrap(theDir->infos, theDir->numEntries);
    delete theDir;
    return 0;
}

/**
 * Creat. Just create the file by opening it. Then set the mode. Finally, add it to our map and
 * return the fd.
 * There's a slight problem here because HDFS doesn't support an atomic open/create. :-(
 * Fortunately, creat does imply truncate, so I don't think it'll be a problem.
 */
int HDFSIOStore::Creat(const char*path, mode_t mode)
{
    int fd;
    string path_string(path);
    hdfsFile file = hdfsOpenFile_retry(fs, path, O_WRONLY, 0, 0, 0);
    if (!file)
        return -1;
    //hdfsChmod_wrap(fs, path, mode);
    
    // We've created the file, set its mode. Now add it to map!
    fd = AddFileToMap(file, O_WRONLY);
    return fd;
}

int HDFSIOStore::Fsync(int fd)
{
    hdfsFile openFile = GetFileFromMap(fd);
    if (!openFile) {
        errno = EBADF;
        return -1;
    }
    return hdfsFlush_wrap(fs, openFile);
}

/**
 * Lseek. Ugh. The tricky part here is the whence parameter.
 * HDFS provides with a seek that only works for an absolute byte offset.
 * The TRICKIER part is this will fail on files open for writes.
 * There's no way around this limitation in HDFS, but fortunately in 
 * PLFS we never update inplace, but always write to the end. So hopefully
 * it won't arise.
 */
off_t HDFSIOStore::Lseek(int fd, off_t offset, int whence)
{
    tOffset new_offset;
    hdfsFile openFile = GetFileFromMap(fd);
    if (!openFile) {
        errno = EBADF;
        return -1;
    }
    switch (whence)  {
    case SEEK_SET:
        new_offset = offset;
        break;
    case SEEK_CUR:
        new_offset = hdfsTell_wrap(fs, openFile) + offset;
        break;
    case SEEK_END:
        // I'm not entirely sure about this call. I'm assuming hdfsAvailable()
        // returns the total number of bytes remaining in the file. If it's
        // less, then this calculation will be wrong. We can definitely get
        // the size through a hdfsGetPathInfo() call. However, this requires
        // a path, not an hdfsFile. This would require storing paths in our
        // map as well. At this point, I'm reluctant to do that.
        new_offset = hdfsTell_wrap(fs, openFile) +
            hdfsAvailable_wrap(fs, openFile) + offset;
        break;
    }

    if (hdfsSeek_wrap(fs, openFile, new_offset)) {
        return -1;
    } 
    return new_offset;
}

/**
 * Lstat. With no symbolic links in HDFS, this is just a call to Stat().
 */
int HDFSIOStore::Lstat(const char* path, struct stat* buf)
{
    return Stat(path, buf);
}

/**
 * A bit easier than Creat since directory creation is pretty straightforward.
 */
int HDFSIOStore::Mkdir(const char* path, mode_t mode)
{
    if (hdfsCreateDirectory_wrap(fs, path)) {
        return -1;
    }

    return hdfsChmod_wrap(fs, path, mode);
}

/**
 * We just create a regular file with this, ignoring dev.
 * So it's just a call to Creat.
 */
int HDFSIOStore::Mknod(const char* path, mode_t mode, dev_t dev) 
{
    return Creat(path, mode);
}

/**
 * MMap is fairly awkward and will go away in favor of a call to replace it
 * for the one task we use it for: Efficiently reading in the index file.
 */
void* HDFSIOStore::Mmap(void *addr, size_t len, int prot, int flags, int fd, off_t offset) 
{
    size_t bytes_read=0;
    char* buffer;
    hdfsFile openFile = GetFileFromMap(fd);
    
    if (!openFile) {
        errno = EBADF;
        return ((void *) -1);
    }

    // We ignore most of the values passed in.
    // Allocate enough space for the lenth.
    buffer = new char[len];

    if (!buffer) {
        errno = ENOMEM;
        return ((void *) -1);
    }
    // Try to read in the file.
    while (bytes_read < len) {
        int res = hdfsPread_wrap(fs, openFile, offset+bytes_read, 
                                 buffer+bytes_read, len-bytes_read);
        if (res == -1) {
            // An error in reading.
            delete[] buffer;
            return ((void *) -1);
        }
        bytes_read += res;
    }

    return buffer;
}

/**
 * Just clean up the allocated space. No real error checking here.
 */
int HDFSIOStore::Munmap(void* addr, size_t len)
{
    char* buffer = (char*)addr;
    delete[] buffer;
    return 0;
}

/**
 * Open is only tricky because HDFS does not support certain modes of operation.
 * For example, we read or write--not both.
 * It really only supports O_RDONLY and O_WRONLY and O_WRONLY|APPEND.
 * (And the last one not really)
 */
int HDFSIOStore::Open(const char* path, int flags)
{
    // TODO: More thought into what the flags should do.
    int new_flags;
    hdfsFile openFile;
    int fd;
    string path_string(path);
    
    if (flags == O_RDONLY) {
        new_flags = O_RDONLY;
    } else if (flags & O_WRONLY)  {
        new_flags = O_WRONLY;
    } else if (flags & O_RDWR) {
        if (!hdfsExists_wrap(fs, path)) {
            // If the file exists, open Read Only!
            flags = O_RDONLY;
        } else {
            flags = O_WRONLY;
        }
    } else {
        //std::cout << "Unsupported flags.\n";
        errno = ENOTSUP;
        return -1;
    }

    //std::cout << "Attempting open on " << path << "\n";
    /*if (hdfsExists_wrap(fs, path)) {
        std::cout << "Doesn't exist yet....\n";
    } else {
        hdfsFileInfo* info = hdfsGetPathInfo_wrap(fs, path);
        std::cout << "Exists and has " << info->mSize << " bytes\n";
        hdfsFreeFileInfo_wrap(info, 1);
    }*/
    openFile = hdfsOpenFile_retry(fs, path, new_flags, 0, 0, 0);
    
    if (!openFile) {
        //std::cout << "Disaster trying to open " << path << "\n";
        return -1;
    }
    fd = AddFileToMap(openFile, new_flags);

    //std::cout << "Opened fd " << fd << "with secret file " << openFile << "\n";
    return fd;
}

/**
 * As the previous open, but with a chmod as well.
 */
int HDFSIOStore::Open(const char* path, int flags, mode_t mode)
{
    int fd = Open(path, flags);
    Chmod(path, mode);

    return fd;
}

/**
 * Opendir. A bit of abuse here... The DIR* functions don't 
 * return a pointer to a directory stream. Instead they return a
 * pointer to a struct openDir, which we use to satisfy future
 * requests.
 * NOTE:
 * There's a slight issue I have with this code--the allocation is
 * in the memory space of the FUSE client, not the application. If the
 * application exits before calling Closedir(), it's a memory leak.
 * I'm not sure what the appropriate technique around this is; even if
 * we reslurp in the entire directory on every call to readdir so we
 * don't need to keep around the array of all file infos, we still need
 * some place to store our counter.
 * One way around this is to replace the entire readdir logic of Util
 * and Container with functionality that stores the contents of the
 * directory in some buffer, and let the higher level manage it. Not
 * sure how that would work with readdir fuse calls, however.
 */
DIR* HDFSIOStore::Opendir(const char *name) 
{
    std::cout << "OpenDir called in HDFS!" << name << "\n";
    openDir* dir = new openDir;
    // I assume new returns NULL on error.
    if (!dir)
        return NULL;
    dir->curEntryNum = 0;
    // We set this to negative one to differentiate between an error and
    // an empty directory.
    dir->numEntries = -1; 
    // We have space to remember the directory. Now slurp in all its files.
    dir->infos = hdfsListDirectory_wrap(fs, name, &dir->numEntries);
    if (!dir->infos && dir->numEntries != 0) {
        delete dir;
        return NULL;
    }
    
    // Temporary debugging measure:
    Util::Debug("Opening a directory with %u entries. Contents:\n",
                dir->numEntries);
    for (int i = 0; i < dir->numEntries; i++) {
        Util::Debug("%s\n", dir->infos[i].mName);
    }

    return (DIR*)dir;
}

/**
 * Rewinddir. Pretty simple. We cast our DIR* into what it really is
 * (as described in Opendir() above) and then set its offset appropriately.
 */
void HDFSIOStore::Rewinddir(DIR* dirp)
{
    struct openDir* dir = (struct openDir*)dirp;
    dir->curEntryNum = 0;
}

/**
 * Pread. A simple wrapper around the HDFS call.
 */
ssize_t HDFSIOStore::Pread(int fd, void* buf, size_t count, off_t offset)
{
    hdfsFile openFile = GetFileFromMap(fd);
    if (!openFile) {
        errno = EBADF;
        return -1;
    }
    //std::cout << "Reading " << count << "bytes\n";
    return hdfsPread_wrap(fs, openFile, offset, buf, count);
}

/**
 * Pwrite. There is no pwrite in HDFS, since we can only write to the end of a
 * file. So this will always fail.
 */
ssize_t HDFSIOStore::Pwrite(int fd, const void* buf, size_t count, off_t offset) 
{
    errno = ENOTSUP;
    return -1;
}

/**
 * Read. A simple wrapper around the HDFS call.
 */
ssize_t HDFSIOStore::Read(int fd, void *buf, size_t count)
{
    hdfsFile openFile = GetFileFromMap(fd);
    if (!openFile) {
        errno = EBADF;
        return -1;
    }
    //std::cout << "Reading " << count << "bytes\n";
    return hdfsRead_wrap(fs, openFile, buf, count);
}

/**
 * Readdir. Remember that the dirp is actually struct openDir*. We cast it
 * and then fill in the struct dirent curEntry embedded in it, using the
 * hdfsFileInfo* infos array and the curEntryNum count.  
 * Then we return the address of that embedded struct dirent.
 * One annoying feature: The mName field of file info is the FULL PATH. So
 * we have to strip it.
 */
struct dirent *HDFSIOStore::Readdir(DIR *dirp)
{
    if (!dirp)
        Util::Debug("HDFSIOStore's Dirp is null... that's probably an issue.\n");
    char* lastComponent; // For locating the last part of the string name.
    struct openDir* dir = (struct openDir*)dirp;
    std::cout << "Just did the cast.\n";
    if (dir->curEntryNum == dir->numEntries) {
        // We've read all the entries! Return NULL.
        Util::Debug("Done reading directory\n");
        return NULL;
    }
    std::cout << "Looked up a field.\n";
    //std::cout << "Processing entry.\n";
    // Fill in the struct dirent curEntry field.
    dir->curEntry.d_ino = 0; // No inodes in HDFS.
    dir->curEntry.d_reclen = sizeof(struct dirent);
    dir->curEntry.d_type = (dir->infos[dir->curEntryNum].mKind == kObjectKindFile
                            ? DT_REG : DT_DIR);

	// I'm not sure how offset is used in this context. Technically
    // only d_name is required in non-Linux deployments.
#ifdef __linux__
    dir->curEntry.d_off = 0;
#endif

    lastComponent = strrchr(dir->infos[dir->curEntryNum].mName, '/');
    if (!lastComponent) { 
        // No slash in the path. Just use the whole thing.
        lastComponent = dir->infos[dir->curEntryNum].mName;
    } else {
        // We want everything AFTER the slash.
        lastComponent++;
    }
    strncpy(dir->curEntry.d_name, lastComponent, NAME_MAX);
    // NAME_MAX does not include the terminating null and if we copy
    // the max number of characters, strncpy won't place it, so set it manually.
    dir->curEntry.d_name[NAME_MAX] = '\0';
    //std::cout << "Entry name " << dir->curEntry.d_name[NAME_MAX];
    dir->curEntryNum++;
    return &dir->curEntry;
}

/**
 * Rename. A wrapper around the hdfs rename call, really.
 */
int HDFSIOStore::Rename(const char *oldpath, const char *newpath)
{
    return hdfsRename_wrap(fs, oldpath, newpath);
}

/**
 * Rmdir. Previously HDFS didn't distinguish between deleting a file and a directory.
 * In 0.21, we do.
 */
int HDFSIOStore::Rmdir(const char* path)
{
    return hdfsDelete_wrap(fs, path, 1);
}
/**
 * Stat. This one is mostly a matter of datat structure conversion to keep everything
 * right.
 */
int HDFSIOStore::Stat(const char* path, struct stat* buf)
{
    hdfsFileInfo* hdfsInfo = hdfsGetPathInfo_wrap(fs, path);
    if (!hdfsInfo) {
        //std::cout << "Failed to get stat for path " << path << "\n";
        errno = ENOENT;
        return -1;
    }
    // Maybe the Hadoop users are the names of an actual user of the system!
    // If so, use that. Otherwise, we'll use 0 for both, which should correspond
    // to root.
    struct group* group_s = getgrnam(hdfsInfo->mGroup);
    struct passwd* passwd_s = getpwnam(hdfsInfo->mOwner); 

    buf->st_dev = 0; // Unused by FUSE, I believe.
    buf->st_ino = 0; // There are no inode numbers in HDFS.

    // We need the mode to be both the permissions and some additional info,
    // based on whether it's a directory of a file.
    buf->st_mode = hdfsInfo->mPermissions;
    if (hdfsInfo->mKind == kObjectKindFile) {
        buf->st_mode |= S_IFREG;
    } else {
        buf->st_mode |= S_IFDIR;
    }
        
    if (passwd_s)
        buf->st_uid = passwd_s->pw_uid;
    else
        buf->st_uid = 0;
    if (group_s)
        buf->st_gid = group_s->gr_gid;
    else
        buf->st_gid = 0;
    buf->st_rdev = 0; // Hopefully unused.
    buf->st_size = hdfsInfo->mSize;
    buf->st_blksize = hdfsInfo->mBlockSize;
    // This is supposed to indicate the actual number of sectors allocated
    // to the file, and is used to calculate storage capacity consumed per
    // File. Files might have holes, but on HDFS we lie about this anyway.
    // So I'm just returning the size/512. Note that if this is supposed to
    // represent storage consumed, it's actually 3 times this due to replication.
    buf->st_blocks = hdfsInfo->mSize/512; 
    buf->st_atime = hdfsInfo->mLastAccess;
    buf->st_mtime = hdfsInfo->mLastMod;
    // This one's a total lie. There's no tracking of metadata change time
    // in HDFS, so we'll just use the modification time again.
    buf->st_ctime = hdfsInfo->mLastMod;
    hdfsFreeFileInfo_wrap(hdfsInfo, 1);
    return 0;
}

/**
 * Statvfs. This one we actually could fill in, but I have not yet done so, as PLFS's
 * logic doesn't depend on it.
 */
int HDFSIOStore::Statvfs( const char *path, struct statvfs* stbuf )
{
	errno = EIO; // This seems the most appropriate, although EACCES is arguable.
	return -1;
}


/** 
 * Symlink. HDFS does not support hard or soft links. So we set errno and return.
 */
int HDFSIOStore::Symlink(const char* oldpath, const char* newpath)
{
    errno = EPERM; // According to POSIX spec, this is the proper code.
    return -1;
}

/**
 * We don't support soft links on HDFS.
 */
ssize_t HDFSIOStore::Readlink(const char*link, char *buf, size_t bufsize)
{
	errno = EINVAL;
	return -1;
}


/**
 * Truncate. In HDFS, we can truncate a file to 0 by re-opening it for O_WRONLY.
 * I'm not sure how this will behave if it's attempted on a file already open.
 * Truncating to anything larger than 0 but smaller than the filesize is impossible.
 * Increasing the filesize can be accomplished by writing 0's to the end of the file,
 * but this requires HDFS to support append, which has always been on and off again
 * functionality in HDFS.
 * For these reasons, we're only supporting it for lengths of 0.
 */
int HDFSIOStore::Truncate(const char* path, off_t length)
{
    hdfsFileInfo *hdfsInfo;
    int lenis0;
    hdfsFile truncFile;

    /* can't partially truncate on HDFS */
    if (length > 0) {
        errno = EINVAL; // EFBIG is also an option.
        return -1;
    }

    hdfsInfo = hdfsGetPathInfo_wrap(fs, path);
    if (!hdfsInfo) {
        /* XXX: libhdfs is supposed to set errno to reasonable value */
        return(-1);
    }
    lenis0 = (hdfsInfo->mSize == 0);
    hdfsFreeFileInfo_wrap(hdfsInfo, 1);

    if (lenis0)
        return(0);   /* already truncated? */

    // BUG: This should really be made atomic with the open call. Otherwise
    // the following sequence is possible:
    // 1. Thread A calls truncate() on 'foo'. It does the hdfsExists() check
    // below and determines that the file exists.
    // 2. Thread B begins running and deletes 'foo'.
    // 3. Thread A begins running again and performs the truncating open call.
    // This creates a new zero-length file.
    // This situation can be avoided with a mutex controlling delete & truncate,
    // but for the time being, I think this is too expensive for the behavior
    // benefits.

    // Attempt to open the file to truncate it.
    truncFile = hdfsOpenFile_retry(fs, path, O_WRONLY, 0, 0, 0);
    if (!truncFile) {
        return -1;
    }

    hdfsCloseFile_wrap(fs, truncFile);

    return 0;
}

/**
 * Unlink. HDFS lacks the concept of links, so this is really just a delete.
 */
int HDFSIOStore::Unlink(const char* path)
{
    return hdfsDelete_wrap(fs, path, 1);
}

/**
 * Utime. More translation mostly.
 */
int HDFSIOStore::Utime(const char* filename, const struct utimbuf *times)
{
    struct utimbuf now;

    Util::Debug("Running utime\n");

    if (times == NULL) {          /* this is allowed, means use current time */
        now.modtime = time(NULL);
        now.actime = time(NULL);
        times = &now;
    }

    return hdfsUtime_wrap(fs, filename, times->modtime, times->actime);
}

/**
 * Write. Similar to read; just lookup the file and issue the write.
 */
ssize_t HDFSIOStore::Write(int fd, const void* buf, size_t len)
{
    hdfsFile openFile = GetFileFromMap(fd);
    if (!openFile) {
        errno = EBADF;
        return -1;
    }
    return hdfsWrite_wrap(fs, openFile, buf, len);
}
