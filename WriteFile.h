#ifndef __WriteFile_H__
#define __WriteFile_H__

#include "COPYRIGHT.h"
#include <map>
using namespace std;
#include "Util.h"
#include "Index.h"
#include "Metadata.h"


class WriteFile : public Metadata {
    public:
        WriteFile( string, string, mode_t ); 
        ~WriteFile();

        int openIndex( pid_t );
        int closeIndex();

        int addWriter( pid_t );
        int removeWriter( pid_t );

        int truncate( off_t offset );
        int extend( off_t offset );

        ssize_t write( const char*, size_t, off_t, pid_t );

        int sync( pid_t pid );

        int restoreFds();
    private:
        int openIndexFile( string path, string host, pid_t, mode_t );
        int openDataFile(string path, string host, pid_t, mode_t );
        int openFile( string, mode_t mode );
        int Close( );
        int closeFd( int fd, const char *type, int pid );
        int getFd( pid_t pid );

        string physical_path;
        string hostname;
        map< pid_t, int  > fds;
        map< int, string > paths;   // need to remember fd paths to restore
        Index *index;
        mode_t mode;
};

#endif
