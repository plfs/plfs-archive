/* -*- Mode: C; c-basic-offset:4 ; -*- */
/* 
 *   $Id: ad_plfs_write.c,v 1.1 2010/11/29 19:59:01 adamm Exp $    
 *
 *   Copyright (C) 1997 University of Chicago. 
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_plfs.h"
#include "adio_extern.h"


void ADIOI_PLFS_WriteStridedColl(ADIO_File fd, void *buf, int count,
                                MPI_Datatype datatype, int file_ptr_type,
                                ADIO_Offset offset, ADIO_Status *status, int
                                *error_code)
{
    ADIOI_Access *my_req; 
    /* array of nprocs access structures, one for each other process in
       whose file domain this process's request lies */
    
    ADIOI_Access *others_req;
    /* array of nprocs access structures, one for each other process
       whose request lies in this process's file domain. */

    int i, filetype_is_contig, nprocs, nprocs_for_coll, myrank,datatype_size;
    int contig_access_count=0, interleave_count = 0, buftype_is_contig;
    int *count_my_req_per_proc, count_my_req_procs, count_others_req_procs;
    ADIO_Offset orig_fp, start_offset, end_offset, fd_size, min_st_offset, off;
    ADIO_Offset *offset_list = NULL, *st_offsets = NULL, *fd_start = NULL,
	*fd_end = NULL, *end_offsets = NULL;
    int *buf_idx = NULL, *len_list = NULL;
    int old_error, tmp_error;
    
    // Lets describe this function
    Plfs_func_desc function; 
    int use_formula = 1; // Boolean

    MPI_Comm_size(fd->comm, &nprocs);
    MPI_Comm_rank(fd->comm, &myrank);

/* the number of processes that actually perform I/O, nprocs_for_coll,
 * is stored in the hints off the ADIO_File structure
 */
    nprocs_for_coll = fd->hints->cb_nodes;
    orig_fp = fd->fp_ind;

    if(myrank==0){
        printf("N procs for coll =%d\n",nprocs_for_coll);
    }

    // Discover the formula for my write
    ADIOI_Calc_my_off_len(fd, count, datatype, file_ptr_type, offset,
			      &offset_list, &len_list, &start_offset,
			      &end_offset, &contig_access_count); 

    int looper;
    for(looper=0;looper<count;looper++){
        printf("Rank[%d],count[%d],offset_list[%d],len_list[%d]\n",
                myrank,looper,offset_list[looper],len_list[looper]);
    }

    /* each process communicates its start and end offsets to other 
       processes. The result is an array each of start and end offsets stored
       in order of process rank. */
    st_offsets = (ADIO_Offset *) ADIOI_Malloc(nprocs*sizeof(ADIO_Offset));
    end_offsets = (ADIO_Offset *) ADIOI_Malloc(nprocs*sizeof(ADIO_Offset));

    MPI_Allgather(&start_offset, 1, ADIO_OFFSET, st_offsets, 1,
                                      ADIO_OFFSET, fd->comm);
    MPI_Allgather(&end_offset, 1, ADIO_OFFSET, end_offsets, 1,
                                          ADIO_OFFSET, fd->comm);

    // First weak test, improve me as we go, need to start work
    // on adding flexibility to the PLFS index. If our start address
    // is equal to our rank * datatype size and end_offset is equal 
    // to count * datatype size + rank * datatype size then we write
    // out a function. For now I am not going to check if this is 
    // true for all ranks, need to check this is true for all ranks
    // if we want to continue, skipping this test for now.
    MPI_Type_size(datatype, &datatype_size);
    int start_check = myrank * datatype_size;
    int end_check = ((count-1) * datatype_size * nprocs) + 
                        start_check + (datatype_size-1);

    // Might want to combine into one compoud if statement
    if( start_check != start_offset ){
        printf("Rank[%d] start offset is not formulaic\n", myrank);
        use_formula=0;
    }
    if( end_check != end_offset ){
        printf("Rank[%d] end offset is not formulaic\n", myrank);
        printf("End check[%d] end_offset[%d]\n",end_check,end_offset);
        use_formula=0;
    }

    if(use_formula){
        // Fill out the initial info we will need for a formula
        if(myrank == 0){
            function.num_procs = nprocs;
            function.start_off = st_offsets[0];
            function.end_off = end_offsets[nprocs-1];
            function.data_size = datatype_size;
            plfs_col_write(fd->fs_ptr, buf, myrank,&function);
        }else{
            plfs_col_write(fd->fs_ptr, buf, myrank,NULL);
        }
    }
}

void ADIOI_PLFS_WriteContig(ADIO_File fd, void *buf, int count, 
			    MPI_Datatype datatype, int file_ptr_type,
			    ADIO_Offset offset, ADIO_Status *status,
			    int *error_code)
{
    int err=-1, datatype_size, len, rank;
    ADIO_Offset myoff;
    static char myname[] = "ADIOI_PLFS_WRITECONTIG";

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
    if (file_ptr_type == ADIO_INDIVIDUAL) {
        myoff = fd->fp_ind;
    }
    plfs_debug( "%s: offset %ld len %ld rank %d\n", 
            myname, (long)myoff, (long)len, rank );
    err = plfs_write( fd->fs_ptr, buf, len, myoff, rank );
#ifdef HAVE_STATUS_SET_BYTES
    if (err >= 0 ) MPIR_Status_set_bytes(status, datatype, err);
#endif

    if (err < 0 ) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
					   myname, __LINE__, MPI_ERR_IO,
					   "**io",
					   "**io %s", strerror(-err));
    } else {
        if (file_ptr_type == ADIO_INDIVIDUAL) {
            fd->fp_ind += err;
        }
        *error_code = MPI_SUCCESS;
    }
}

