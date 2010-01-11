/* -*- Mode: C; c-basic-offset:4 ; -*- */
/* 
 *   $Id: ad_plfs_read.c,v 1.15 2004/10/07 16:15:17 rross Exp $    
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
    int err=-1, datatype_size, len ;
    static char myname[] = "ADIOI_PLFS_READCONTIG";

    MPI_Type_size(datatype, &datatype_size);
    len = datatype_size * count;

    if (file_ptr_type != ADIO_EXPLICIT_OFFSET) {
        offset = fd->fp_ind;
    }

    err = plfs_read( fd->fs_ptr, buf, len, offset );

#ifdef HAVE_STATUS_SET_BYTES
    if (err >= 0 ) MPIR_Status_set_bytes(status, datatype, err);
#endif

    if (err < 0 ) {
	*error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
					   myname, __LINE__, MPI_ERR_IO,
					   "**io",
					   "**io %s", strerror(-err));
    } else {
        *error_code = MPI_SUCCESS;
    }
}
