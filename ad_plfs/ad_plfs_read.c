/* -*- Mode: C; c-basic-offset:4 ; -*- */
/* 
 *   $Id: ad_plfs_read.c,v 1.1 2010/11/29 19:59:01 adamm Exp $    
 *
 *   Copyright (C) 1997 University of Chicago. 
 *   See COPYRIGHT notice in top-level directory.
 */

#include "adio.h"
#include "adio_extern.h"
#include "ad_plfs.h"

void ADIOI_PLFS_ReadContig(ADIO_File fd, void *buf, int count, 
                     MPI_Datatype datatype, int file_ptr_type,
		     ADIO_Offset offset, ADIO_Status *status, int *error_code)
{
    int err=-1, datatype_size, len, rank;
    ADIO_Offset myoff;
    static char myname[] = "ADIOI_PLFS_READCONTIG";

    MPI_Type_size(datatype, &datatype_size);
    len = datatype_size * count;
    MPI_Comm_rank( fd->comm, &rank );

    // for the romio/test/large_file we always get an offset of 0
    // maybe we need to increment fd->fp_ind ourselves?
    if (file_ptr_type == ADIO_EXPLICIT_OFFSET) {
        myoff = offset;
    } else {
        myoff = fd->fp_ind;
    }
    plfs_debug( "%s: offset %ld len %ld rank %d\n", 
            myname, (long)myoff, (long)len, rank );

    err = plfs_read( fd->fs_ptr, buf, len, myoff );

#ifdef HAVE_STATUS_SET_BYTES
    if (err >= 0 ) MPIR_Status_set_bytes(status, datatype, err);
#endif

    if (err < 0 ) {
	*error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
					   myname, __LINE__, MPI_ERR_IO,
					   "**io",
					   "**io %s", strerror(-err));
    } else {
        fd->fp_ind += err;
        *error_code = MPI_SUCCESS;
    }
}

void ADIOI_PLFS_ReadStrided(ADIO_File fd, void *buf, int count,
                            MPI_Datatype datatype, int file_ptr_type,
                            ADIO_Offset offset, ADIO_Status *status, 
                            int *error_code){
    
    int err=-1, buftype_size,rank,filetype_size;
    int buftype_is_contig,filetype_is_contig;

    ADIO_Offset myoff;
    long int bufsize;
    static char myname[] = "ADIOI_PLFS_READCONTIG";

    MPI_Comm_rank( fd->comm, &rank );
    MPI_Type_size(datatype, &buftype_size);
    
    bufsize = buftype_size * count;
    myoff = rank * bufsize;

    // Lets figure out if the buffer and filetype are contiguous
    ADIOI_Datatype_iscontig(datatype, &buftype_is_contig);
    ADIOI_Datatype_iscontig(fd->filetype, &filetype_is_contig);

    MPI_Type_size(fd->filetype, &filetype_size);
    if ( ! filetype_size ) {
        *error_code = MPI_SUCCESS;
        return;
    }

    if (buftype_is_contig && !filetype_is_contig) {
        /* contiguous in memory, noncontiguous in file. should be the most
                                         common case. */
            plfs_read( fd->fs_ptr,buf,bufsize,myoff);

    }

                    
}
