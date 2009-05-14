#include "OpenFile.h"

OpenFile::OpenFile( WriteFile *wf, Index *i, pid_t p ) :
        Metadata::Metadata() 
{
    this->writefile = wf;
    this->index     = i;
    this->pid       = p;
    this->write_data_fd  = -1;
    this->write_index_fd = -1;
    this->write_index    = NULL;
}

WriteFile *OpenFile::getWritefile( ) {
    return writefile;
}

Index *OpenFile::getIndex( ) {
    return index;
}

pid_t OpenFile::getPid() {
    return pid;
}

void OpenFile::getWriteFds( int *d, int *i, Index **I ) {
    *d = write_data_fd;
    *i = write_index_fd;
    *I = write_index;
}

void OpenFile::setWriteFds( int d, int i, Index *I ) {
    write_data_fd  = d;
    write_index_fd = i;
    write_index    = I;
}
