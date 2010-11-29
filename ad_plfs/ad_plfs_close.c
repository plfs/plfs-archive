/* -*- Mode: C; c-basic-offset:4 ; -*- */
/* 
 *   $Id: ad_plfs_close.c,v 1.1 2010/11/29 19:59:01 adamm Exp $    
 *
 *   Copyright (C) 1997 University of Chicago. 
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_plfs.h"

int flatten_on_close(Plfs_fd *fd,int rank,int amode,int procs,
            Plfs_close_opt *close_opt);
extern off_t last_offset;
extern size_t total_bytes;

void ADIOI_PLFS_Close(ADIO_File fd, int *error_code)
{
    int err, rank, amode,procs;
    static char myname[] = "ADIOI_PLFS_CLOSE";
    Plfs_close_opt close_opt;
    int flatten=0;
    off_t glbl_lst_off=0;
    size_t glbl_tot_byt=0;
    
    plfs_debug("%s: begin\n", myname );

    MPI_Comm_rank( MPI_COMM_WORLD, &rank );
    MPI_Comm_size( MPI_COMM_WORLD, &procs);

    amode = ad_plfs_amode( fd->access_mode );

    if(fd->fs_ptr==NULL) {
        // ADIO does a weird thing where it 
        // passes CREAT to just 0 and then
        // immediately closes.  When we handle
        // the CREAT, we put a NULL in
        *error_code = MPI_SUCCESS;
        return;
    }
    // Grab the last offset from all ranks and reduce to the max
    MPI_Reduce(&last_offset,&glbl_lst_off,1,MPI_LONG_LONG,MPI_MAX,0,MPI_COMM_WORLD);
    // Grab the total bytes from all ranks and reduce to the sum
    MPI_Reduce(&total_bytes,&glbl_tot_byt,1,MPI_LONG_LONG,MPI_SUM,0,MPI_COMM_WORLD);
    // Set up our struct to pass the last off and tot bytes to the close
    close_opt.last_offset=glbl_lst_off;
    close_opt.total_bytes=glbl_tot_byt;
    flatten = ad_plfs_hints ( fd , rank, "plfs_flatten_close"); 
    // Only want to do this on write
    if(flatten && fd->access_mode!=ADIO_RDONLY) {
        err = flatten_on_close(fd->fs_ptr, rank, amode, procs, &close_opt);
    }
    else{
        err = plfs_close(fd->fs_ptr, rank, amode,&close_opt);
    }
    
    fd->fs_ptr = NULL;

    if (err < 0 ) {
	*error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
					   myname, __LINE__, MPI_ERR_IO,
					   "**io",
					   "**io %s", strerror(-err));
    } else {
         *error_code = MPI_SUCCESS;
    }
}


int flatten_on_close(Plfs_fd *fd,int rank,int amode,int procs,Plfs_close_opt *close_opt){
    
    int index_size,err,index_total_size=0,streams_malloc=1,stop_buffer=0;
    int *index_sizes,*index_disp;
    char *index_stream,*index_streams;
    double start_time,end_time;

    // Get the index stream from the local index
    index_size=plfs_index_stream(&(fd),&index_stream);
    // Malloc space to receive all of the index sizes
    // Do all procs need to do this? I think not
    if(!rank) {
        index_sizes=(int *)malloc(procs*sizeof(int));
        if(!index_sizes) {
            plfs_debug("Malloc failed:index size gather\n");
            MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
        }
    }

    if(!rank) start_time=MPI_Wtime();
    // Perform the gather of all index sizes to set up our vector call
    MPI_Gather(&index_size,1,MPI_INT,index_sizes,1,MPI_INT,0,MPI_COMM_WORLD);
    // Figure out how much space we need and then malloc if we are root
    if(!rank){
        end_time=MPI_Wtime();
        plfs_debug("Gather of index sizes time:%.12f\n"
                    ,end_time-start_time);
        int count;
        // Malloc space for out displacements
        index_disp=malloc(procs*sizeof(int));
        if(!index_disp){
            plfs_debug("Displacements malloc has failed\n");
            MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO);
        }
        for(count=0;count<procs;count++){
            // Set up displacements 
            if(count==0){
                index_disp[count]=0;
            }else{
                // Displacements 
                index_disp[count]=index_total_size-(count);
            }
            // Calculate the size of the index
            index_total_size+=index_sizes[count];
            if(index_sizes[count]==-1) {
                plfs_debug("Rank %d had an index that wasn't buffered\n",count);
                stop_buffer=1;
            }
        }
        plfs_debug("Total size of indexes %d\n",index_total_size);
        index_streams=(char *)malloc((index_total_size*sizeof(char)));
        if(!index_streams){
            plfs_debug("Malloc failed:index streams\n");
            streams_malloc=0;
        }
    }
    
    /*err=MPI_Bcast(&streams_malloc,1,MPI_INT,0,MPI_COMM_WORLD);

    if(err != MPI_SUCCESS){
        int resultlen; 
        char err_buffer[MPI_MAX_ERROR_STRING]; 
        MPI_Error_string(err,err_buffer,&resultlen); 
        printf("Error:%s | Rank:%d\n",err_buffer,rank); 
        MPI_Abort(MPI_COMM_WORLD,MPI_ERR_IO); 
    }
    */
    if(!rank) start_time=MPI_Wtime();
    
    // Gather all of the subindexes only if malloc succeeded
    // and no one stopped buffering
    if( streams_malloc && !stop_buffer){
        MPI_Gatherv(index_stream,index_size,MPI_CHAR,index_streams,
                    index_sizes,index_disp,MPI_CHAR,0,MPI_COMM_WORLD);
    }

    if(!rank) {
        end_time=MPI_Wtime();
        plfs_debug("Gatherv of indexes:%.12f\n"
                    ,end_time-start_time);
    }
    // We are root lets combine all of our subindexes
    if(!rank && streams_malloc && !stop_buffer){
        plfs_debug("About to merge indexes\n");
        start_time=MPI_Wtime();
        plfs_merge_indexes(&(fd),index_streams,index_sizes,procs);
        end_time=MPI_Wtime();
        plfs_debug("Finished merging indexes time:%.12f\n"
                    ,end_time-start_time);
    }
    // Close normally
    // This should be fine before the previous if statement
    err = plfs_close(fd, rank, amode,close_opt);
    
    //free(index_stream);
    
    if(!rank){
        // Only root needs to complete these frees
        free(index_sizes);
        free(index_disp);
        free(index_streams);
    }
    // Everyone needs to free their index stream
    // Root doesn't really need to make this call
    // Could take out the plfs_index_stream call for root
    // This is causing errors does the free to index streams clean this up?

    return err;
}
