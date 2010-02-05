#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <iostream>
//#include "plfs.h"
//#include "PosixIOStore.h"
//#include "Util.h"
#include "hdfs.h"

int main(int argc, char **argv) {
    Plfs_fd *myFile = NULL;
    char *dataBuffer = "Hello PLFS!\n"; // 12 chars + null
    char *indexBuffer = "00.00.12"; // A fake representation of an index entry. 8 + null.
    char inputBuffer[13];
    ssize_t bytes;

    hdfsFile indexFile;
    hdfsFile dataFile;

    if (argc != 2) {
        std::cout << "Usage: " << argv[0] << "<path to test file>\n";
        return -1; 
    }    
    
    // Connect to whichever filesystem is specified in the site configuration or
    // the local filesystem if there's no Hadoop configuration directory in the
    // $CLASSPATH
    hdfsFS fs = hdfsConnect("default", 0);

    // Make the directory/container
    if (hdfsCreateDirectory(fs, argv[1])) {
        std::cout << "Error creating container directory.\n";
        return -1;
    }

    // Create the data file and the index file.
    if (hdfsSetWorkingDirectory(fs, argv[1])) {
        std::cout << "Error changing directory.\n";
        return -1;
    }

    indexFile = hdfsOpenFile(fs, "index", O_WRONLY, 0, 0, 0);
    dataFile = hdfsOpenFile(fs, "data", O_WRONLY, 0, 0, 0);

    if (!indexFile || !dataFile) {
        std::cout << "Error opening files for writing!";
        return -1;
    }

    // Set the file modes to what they are in the PLFS version, just to be
    // sure things are comparable.
    hdfsChmod(fs, "index", 0777);
    hdfsChmod(fs, "data", 0777);

    // Write into them!
    bytes = hdfsWrite(fs, dataFile, dataBuffer, 12);
    bytes = hdfsWrite(fs, indexFile, indexBuffer, 8);

    // Close them.
    if (hdfsCloseFile(fs, dataFile)) {
        std::cout << "Error closing datafile.\n";
        return -1;
    }

    if (hdfsCloseFile(fs, indexFile)) {
        std::cout << "Error closing indexfile.\n";
        return -1;
    }

    // Re-open for read.
    indexFile = hdfsOpenFile(fs, "index", O_RDONLY, 0, 0, 0);
    dataFile = hdfsOpenFile(fs, "data", O_RDONLY, 0, 0, 0);

    if (!indexFile || !dataFile) {
        std::cout << "Error opening files for reading!";
        return -1;
    }
    
    bytes = hdfsRead(fs, indexFile, inputBuffer, 8);
    if (8 != bytes) {
        std::cout << "Error reading index file\n";
        return -1;
    }
    bytes = hdfsRead(fs, dataFile, inputBuffer, 12);
    if (12 != bytes) {
        std::cout << "Error reading data file.\n";
        return -1;
    }

    return 0;
}
