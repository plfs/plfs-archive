#include "OpenFile.h"
#include "COPYRIGHT.h"

Plfs_fd::Plfs_fd( WriteFile *wf, Index *i, pid_t p ) :
        Metadata::Metadata() 
{
    this->writefile = wf;
    this->index     = i;
    this->pid       = p;
    this->write_data_fd  = -1;
    this->write_index_fd = -1;
    this->write_index    = NULL;
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

void Plfs_fd::getWriteFds( int *d, int *i, Index **I ) {
    *d = write_data_fd;
    *i = write_index_fd;
    *I = write_index;
}

void Plfs_fd::setWriteFds( int d, int i, Index *I ) {
    write_data_fd  = d;
    write_index_fd = i;
    write_index    = I;
}
