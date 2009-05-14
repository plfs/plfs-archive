#include <strings.h>
#include <pthread.h>
#include <iostream>
#include <iomanip>
#include <fstream>
using namespace std;
#include "LogMessage.h"

#define LOG_BUFFER_SZ 200

ofstream        *log_stream;
pthread_mutex_t  log_mutex;
bool             std_err;
string           log_array[LOG_BUFFER_SZ];
long unsigned    log_array_index;   // ptr to current input point
long unsigned    log_array_size;    // number of valid entries

int LogMessage::init( const char *logfile ) {
    int ret = 0;
    log_array_index = 0;
    log_array_size  = 0;
    if ( logfile && strcasecmp(logfile,"null") != 0 ) {
        pthread_mutex_init( &log_mutex, NULL );
        if ( strcasecmp( logfile, "/dev/stderr" ) == 0 ) {
            std_err = true;
            log_stream = NULL;
            ret = 1;
        } else {
            std_err = false;
            log_stream = new ofstream( logfile );
            if ( ! log_stream ) {
                cerr << "Failed to open logfile " << logfile << endl;
            } else {
                cerr << "Opened logfile " << logfile << endl;
            }
            ret = ( log_stream ? 1 : 0 );
        }
    } else {
        log_stream = NULL;
        std_err     = false;
        ret = 1;
    }
    return ret;
}

int LogMessage::changeLogFile( const char *logfile ) {
    if ( log_stream ) {
        Flush();
        log_stream->close();
        delete log_stream; 
        log_stream = NULL;
    }
    return init( logfile );
}
        
void LogMessage::Flush() {
    if ( log_stream ) {
        log_stream->flush();
    }
}

string LogMessage::Dump() {
    ostringstream dump;
    long unsigned start_index   = 0;
    long unsigned valid_entries = 0;
    if ( log_array_size >= LOG_BUFFER_SZ ) {
        start_index = log_array_index;
        valid_entries = LOG_BUFFER_SZ;
    } else {
        start_index = 0;
        valid_entries = log_array_size;
    }
    cerr << "Set start_index " << start_index << " and valid_entries " 
         << valid_entries << endl;
    for( long unsigned i = 0; i < valid_entries; i++ ) {
        dump << log_array[(i+start_index)%LOG_BUFFER_SZ];
    }
    return dump.str();
}

void LogMessage::flush() {

    pthread_mutex_lock( &log_mutex );
    log_array[log_array_index] = this->str();
    log_array_index = (log_array_index + 1) % LOG_BUFFER_SZ;
    log_array_size  = min( log_array_size + 1, (long unsigned)LOG_BUFFER_SZ );
    if ( log_stream ) {
        log_stream->write( this->str().c_str(), this->str().size() );
    }
    pthread_mutex_unlock( &log_mutex );

    if ( std_err ) {
        cerr << this->str();
    }
    this->str().resize(0);
    //cerr << "Erased my string "<< " new size is " << this->str().size() << endl;
    this->clear();
}

void LogMessage::addTime( double t ) {
    *this << setw(22) << setprecision(22) << t << " ";
}
        
void LogMessage::addIds( uid_t uid, gid_t gid ) {
    *this << " UID " << uid << " GID " << gid;
}

void LogMessage::addPid( pid_t pid ) {
    *this << " PID " << pid;
}

void LogMessage::addOff( off_t off ) {
    *this << " offset " << off;
}

void LogMessage::addSize( size_t size ) {
    *this << " size " << size;
}

void LogMessage::addFunction( const char * func ) {
    *this << setw(13) << func << " ";
}
