#include "PhysicalLogfile.h"
#include "Util.h"
#include "mlogfacs.h"
#include "plfs_private.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>

PhysicalLogfile::PhysicalLogfile(const string &path) {
    size_t bufsz = 0;
    bufsz = get_plfs_conf()->data_buffer_mbs; 
    init(path,bufsz*1048576);
}


PhysicalLogfile::PhysicalLogfile(const string &path, size_t bufsz) {
    init(path,bufsz);
}

void
PhysicalLogfile::init(const string &path, size_t bufsz) {
    this->bufoff = 0;
    this->path   = path;
    this->fd     = -1;
    if (bufsz) {
        buf = malloc(bufsz);
        if (!buf) {
            mlog(PLOG_WARN, "WTF: Malloc of %ld in %s:%s failed\n",
                    (long)bufsz,__FILE__,__FUNCTION__);
            this->bufsz = 0;
        } else {
            this->bufsz = bufsz;
        }
    } else {
        buf = NULL;
    }
    mlog(PLOG_DAPI, "Successfully init'd plogfile %s\n",path.c_str());
}

PhysicalLogfile::~PhysicalLogfile() {
    if (fd > 0) this->close();  
    if (buf)    free(buf);
}

int
PhysicalLogfile::close() {
    assert(fd > 0); // someone trying to close already closed
    int ret1 = this->flush();
    int ret2 = Util::Close(fd);
    return ret1 != 0 ? ret1 : ret2;
}

// returns 0 or -errno
int
PhysicalLogfile::open(mode_t mode) {
    assert(fd==-1); // someone attempting to reopen already open file
    int flags = O_WRONLY | O_APPEND | O_CREAT | O_TRUNC;
    fd = Util::Open(path.c_str(),flags,mode);
    return ( fd>0 ? 0 : -errno );
}

int
PhysicalLogfile::flush() {
    int ret = 0;
    if (bufoff > 0) {   
        mlog(PLOG_DAPI, "Flushing %d bytes of %s\n", (int)bufoff, path.c_str());
        ret = Util::Write(fd,buf,bufoff); 
        if (ret>0) {    // success.  reset offset.
            bufoff = 0;
            ret = 0;    // return 0
        }
    }
    return ret;
}

int
PhysicalLogfile::sync() {
    int ret = this->flush(); 
    if (ret == 0) { 
        return Util::Fsync(fd);
    }
    return ret;
}

ssize_t 
PhysicalLogfile::append(const void *src, size_t nbyte) {

    // if we're not buffering, this is easy!
    if (!buf) return Util::Write(fd,src,nbyte);    

    // this would be easy if we knew that the data never overflowed the buf...
    int ret = 0;
    size_t remaining = nbyte;
    size_t src_off   = 0;

    while(ret==0 && remaining) {
        // copy as much as we can into our buffer
        size_t avail = bufsz - bufoff;
        size_t thiscopy = min(avail,remaining);
        char *bufptr, *srcptr;
        bufptr = (char *)buf;
        bufptr += bufoff;
        srcptr = (char *)src;
        srcptr += src_off;
        memcpy(bufptr,srcptr,thiscopy);

        // now update our state
        bufoff += thiscopy;
        remaining -= thiscopy;
        assert(remaining>=0);

        // if buffer is full, flush it
        if (bufoff >= bufsz) {
            assert(bufoff==bufsz);
            ret = this->flush();
            bufoff = 0; // flush does this but just to be thorough....
        }
    }

    if (ret >= 0 ) {
        mlog(PLOG_DAPI, "Appended %lu to %s\n",
            (unsigned long)nbyte, path.c_str());
        return nbyte;
    } else {
        mlog(PLOG_DAPI, "Failed to append %lu to %s: %s\n",
            (unsigned long)nbyte, path.c_str(), strerror(-ret));
        return ret;
    }
}
