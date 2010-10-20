#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "plfs.h"
#include "COPYRIGHT.h"

int main (int argc, char **argv) {
    const char *target  = argv[1];
    Plfs_path_type path_type=LOGICAL_PATH;
    int ret = plfs_flatten_index(NULL,target,path_type);
    if ( ret != 0 ) {
        fprintf( stderr, "Couldn't read index from %s: %s\n", 
                target, strerror(-ret));
    } else {
        printf("Successfully flattened index of %s\n",target);
    }
    exit( ret );
}
