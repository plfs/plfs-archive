#ifndef __OpenFile_H__
#define __OpenFile_H__
#include "COPYRIGHT.h"
#include <string>
#include <map>
#include "WriteFile.h"
#include "Index.h"
#include "Metadata.h"
using namespace std;

#define PPATH 1024

class OpenFile : public Metadata {
    public:
        OpenFile( WriteFile *, Index *, pid_t );
        WriteFile *getWritefile();
        Index     *getIndex();
        void      setWriteFds( int, int, Index * );
        void      getWriteFds( int *, int *, Index ** );
        pid_t     getPid();
    private:
        WriteFile *writefile;
        Index     *index;
        pid_t     pid;

            // also stash a copy of the stuff we need for writing in here
            // this way, we won't have to lock a mutex in f_write in order
            // to retrieve it
        Index     *write_index;
        int       write_data_fd;
        int       write_index_fd;
};

#endif
