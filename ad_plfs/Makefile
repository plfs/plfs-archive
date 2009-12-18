CC          = /Users/johnbent/Testing/mpich_plfs/mpich-1.2.7/bin/mpicc
AR          = ar cr
RANLIB      = ranlib
LIBNAME     = /Users/johnbent/Testing/mpich_plfs/mpich-1.2.7/lib/libmpich.a
srcdir      = /Users/johnbent/Testing/mpich_plfs/mpich-1.2.7/romio/adio/ad_plfs
CC_SHL      = 
SHLIBNAME   = /Users/johnbent/Testing/mpich_plfs/mpich-1.2.7/lib/libmpich
PLFSLIB     = ~/Testing/plfs/branches/library/src

INCLUDE_DIR = -I. -I${srcdir}/../include -I../include -I../../include  -I${srcdir}/../../../../include -I../../../../include -I$(PLFSLIB)
CFLAGS      =  -DMPID_NO_FORTRAN  -I/Users/johnbent/Testing/plfs/branches/library/src -DUSE_SOCKLEN_T -DHAVE_MPICHCONF_H -DMPID_NO_FORTRAN  -I/Users/johnbent/Testing/plfs/branches/library/src -DUSE_SOCKLEN_T -DHAVE_MPICHCONF_H -DHAVE_ROMIOCONF_H $(INCLUDE_DIR)

top_builddir  = /Users/johnbent/Testing/mpich_plfs/mpich-1.2.7/romio
LIBTOOL       = 
C_COMPILE_SHL = $(CC_SHL) 

VPATH = .:${srcdir}

AD_PLFS_OBJECTS = ad_plfs_close.o ad_plfs_read.o \
      ad_plfs_open.o ad_plfs_write.o \
      ad_plfs_flush.o ad_plfs_resize.o \
      ad_plfs_delete.o ad_plfs.o

default: $(LIBNAME)
	@if [ "none" != "none" ] ; then \
	    $(MAKE) $(SHLIBNAME).la ;\
	fi

.SUFFIXES: $(SUFFIXES) .p .lo

.c.o:
	$(CC) $(CFLAGS) -c $<
.c.lo:
	$(C_COMPILE_SHL) $(CFLAGS) -c $<
	@mv -f $*.o $*.lo

$(LIBNAME): $(AD_PLFS_OBJECTS)
	$(AR) $(LIBNAME) $(AD_PLFS_OBJECTS)
	$(RANLIB) $(LIBNAME) 

AD_PLFS_LOOBJECTS=$(AD_PLFS_OBJECTS:.o=.lo)
$(SHLIBNAME).la: $(AD_PLFS_LOOBJECTS)
	$(AR) $(SHLIBNAME).la $(AD_PLFS_LOOBJECTS)
	$(RANLIB) $(SHLIBNAME) 

coverage:
	-@for file in  ${AD_PLFS_OBJECTS:.o=.c} ; do \
		gcov -b -f $$file ; done

clean:
	@rm -f *.o
