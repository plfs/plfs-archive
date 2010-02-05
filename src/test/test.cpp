#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <iostream>
#include "plfs.h"
#include "PosixIOStore.h"
#include "Util.h"

int main() {
    Plfs_fd *myFile = NULL;
    char *buffer = new char[13];
    int bytes;
    plfs_hdfs_init("default", 0);
    //plfs_posix_init();
    std::cout << "Inited\n";
    plfs_create("./foo4", 0777, 0);
    std::cout << "Created.\n";
    plfs_open(&myFile, "./foo4", O_WRONLY, 2, 0777);
    std::cout << "Opened for write.\n";
    plfs_write (myFile, "Hello World\n", 12, 0, 2);
    std::cout << "Written.\n";
    plfs_close(myFile, 2);
    std::cout << "Closed.\n";
    myFile = NULL;
    if (plfs_open(&myFile, "./foo4", O_RDONLY, 2, 0777)) {
        std::cout << "Failed to open for read!\n";
    } else {
        std::cout << "Opened for read.\n";
    }
    if (-1 == (bytes = plfs_read(myFile, buffer, 12, 0))) {
        std::cout << "Failed to read! " << errno << "\n";
    } else {
        std::cout << "Read " << bytes << " bytes: " << buffer << "\n";
    }
    plfs_close(myFile, 2);
    std::cout << "Closed.\n";
    return 0;
}
