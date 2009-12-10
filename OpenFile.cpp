#include "OpenFile.h"
#include "COPYRIGHT.h"

PlfsFd::PlfsFd( WriteFile *wf, Index *i, pid_t p ) :
        Metadata::Metadata() 
{
    this->writefile = wf;
    this->index     = i;
    this->pid       = p;
    this->write_data_fd  = -1;
    this->write_index_fd = -1;
    this->write_index    = NULL;
}

WriteFile *PlfsFd::getWritefile( ) {
    return writefile;
}

Index *PlfsFd::getIndex( ) {
    return index;
}

pid_t PlfsFd::getPid() {
    return pid;
}

void PlfsFd::getWriteFds( int *d, int *i, Index **I ) {
    *d = write_data_fd;
    *i = write_index_fd;
    *I = write_index;
}

void PlfsFd::setWriteFds( int d, int i, Index *I ) {
    write_data_fd  = d;
    write_index_fd = i;
    write_index    = I;
}
