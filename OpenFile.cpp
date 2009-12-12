#include "OpenFile.h"
#include "COPYRIGHT.h"

Plfs_fd::Plfs_fd( WriteFile *wf, Index *i, pid_t pi, mode_t m, const char *p ) :
        Metadata::Metadata() 
{
    this->writefile = wf;
    this->index     = i;
    this->pid       = pi;
    this->path      = p;
    this->mode      = m;
}

WriteFile *Plfs_fd::getWritefile( ) {
    return writefile;
}

Index *Plfs_fd::getIndex( ) {
    return index;
}

pid_t Plfs_fd::getPid() {
    return pid;
}
