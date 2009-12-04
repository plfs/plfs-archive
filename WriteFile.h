#ifndef __WriteFile_H__
#define __WriteFile_H__

#include "COPYRIGHT.h"
#include <map>
using namespace std;
#include "Util.h"
#include "Index.h"
#include "Metadata.h"

typedef struct {
    int datafd;
    int indexfd;
    Index *index;
} FDS;

class WriteFile : public Metadata {
    public:
        WriteFile( string, string, mode_t ); 
        ~WriteFile();

        int getFds( pid_t pid, mode_t, 
                int *indexfd, int *datafd, Index **index );

            // should close before destroy so that errors can be returned
        int Close( );

        int Close( int pid );

        int Remove( int pid );

        int  getIndexFd()          { return this->indexfd; }

        const char *getHost()      { return this->hostname.c_str(); } 

        void setMode( mode_t m )   { this->mode = m; }
        mode_t getMode()           { return this->mode; }

        int truncate( off_t offset );
        static int openIndexFile( string path, string host, mode_t mode );
        static int closeIndexFile( int fd );

    private:
        static int openDataFile(string path, string host, pid_t, mode_t );
        static int openFile( string, mode_t mode );
        string getIndexDataPath( const char *, int );
        int closeFd( int fd, const char *type, int pid );
        int closeFds( FDS fds, int pid );

        string physical_path;
        string hostname;
        map< int, FDS > fd_map;
        int indexfd;    // a write file now has a shared index fd
        mode_t mode;
};

#endif
