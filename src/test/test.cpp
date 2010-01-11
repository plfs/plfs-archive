#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "plfs.h"
#include "PosixIOStore.h"
#include "Util.h"

int main() {
    Plfs_fd *myFile;
    //PosixIOStore pios;
    //Util::ioStore = &pios;

    plfs_posix_init();

    plfs_open(&myFile, "./foo", O_CREAT | O_WRONLY, 2, 0666);

    plfs_write (myFile, "Hello World\n", 6, 0, 2);

    plfs_close(myFile, 2);

    return 0;
}
