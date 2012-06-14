/*
 *   Copyright 2011 Cray Inc. All Rights Reserved.
 */
/* 
 *   Copyright (C) 2008 University of Chicago. 
 *   See COPYRIGHT notice in top-level directory.
 */

#include "adio.h"

int ADIOI_PLFS_Feature(ADIO_File fd, int flag)
{
	switch(flag) {
		case ADIO_LOCKS:
		case ADIO_SHARED_FP:
		case ADIO_ATOMIC_MODE:
		case ADIO_DATA_SIEVING_WRITES:
		/* In the case of PLFS, "ADIO_SCALABLE_OPEN==TRUE" translates
		 * to "don't do deferred open". */
		case ADIO_SCALABLE_OPEN:
			return 1;
			break;
		default:
			return 0;
			break;
	}
}

