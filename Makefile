LIBRARY    = libplfs.a
SRCS       = $(wildcard *.cpp) 
HDRS       = $(wildcard *.h)
OBJS       = $(addsuffix .o, $(basename $(SRCS) ) ) 

# put -DNDEBUG in to turn off assertions, this can make a big difference
# as many of the asserts are doing stats
# put -DNUTIL in to turn off timing in Util
# add -DPLFS_TIMES to turn on timing in plfs
# add -DINDEX_CONTAINS_TIMESTAMPS to allow trace vizualization
# add -DCOUNT_SKIPS to maintain a count of sequential / non-sequential IO 
DEBUG   = -DNDEBUG -DPLFS_TIMES -DNUTIL -DINDEX_CONTAINS_TIMESTAMPS \
	      -DCOUNT_SKIPS
CFLAGS  = -g -Wall $(DEBUG) -D_FILE_OFFSET_BITS=64
LDFLAGS = 

UNAME		:= $(shell uname)

ifeq ($(UNAME), Darwin)
	CFLAGS += -D__FreeBSD__=10
endif

all: $(LIBRARY)

$(LIBRARY): $(OBJS) 
	ar rcs $@ $(OBJS) 

%.o: %.C $(HDRS)
	g++ $(CFLAGS) -o $@ -c $<

%.o : %.cpp $(HDRS)
	g++ $(CFLAGS) -o $@ -c $<
 
clean:
	/bin/rm -f $(LIBRARY) *.o
# DO NOT DELETE

plfs.o: plfs.h /usr/include/sys/types.h /usr/include/sys/appleapiopts.h
plfs.o: /usr/include/sys/cdefs.h /usr/include/machine/types.h
plfs.o: /usr/include/i386/types.h /usr/include/i386/_types.h
plfs.o: /usr/include/sys/_types.h /usr/include/machine/_types.h
plfs.o: /usr/include/machine/endian.h /usr/include/i386/endian.h
plfs.o: /usr/include/sys/_endian.h /usr/include/libkern/_OSByteOrder.h
plfs.o: /usr/include/libkern/i386/_OSByteOrder.h /usr/include/sys/_structs.h
plfs.o: plfs_private.h Index.h COPYRIGHT.h Util.h /usr/include/sys/stat.h
plfs.o: /usr/include/dirent.h /usr/include/_types.h /usr/include/sys/dirent.h
plfs.o: /usr/include/errno.h /usr/include/sys/errno.h /usr/include/fcntl.h
plfs.o: /usr/include/sys/fcntl.h /usr/include/sys/dir.h
plfs.o: /usr/include/sys/syscall.h /usr/include/sys/param.h
plfs.o: /usr/include/sys/syslimits.h /usr/include/machine/param.h
plfs.o: /usr/include/i386/param.h /usr/include/i386/_param.h
plfs.o: /usr/include/limits.h /usr/include/machine/limits.h
plfs.o: /usr/include/i386/limits.h /usr/include/i386/_limits.h
plfs.o: /usr/include/sys/signal.h /usr/include/machine/signal.h
plfs.o: /usr/include/i386/signal.h /usr/include/i386/_structs.h
plfs.o: /usr/include/sys/mount.h /usr/include/sys/attr.h
plfs.o: /usr/include/sys/ucred.h /usr/include/bsm/audit.h
plfs.o: /usr/include/sys/queue.h /usr/include/sys/socket.h
plfs.o: /usr/include/machine/_param.h /usr/include/sys/time.h
plfs.o: /usr/include/time.h /usr/include/_structs.h
plfs.o: /usr/include/sys/_select.h /usr/include/stdint.h
plfs.o: /usr/include/sys/statvfs.h Metadata.h WriteFile.h Container.h
plfs.o: OpenFile.h
