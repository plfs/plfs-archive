TARGET     = plfs
LOG        = /tmp/plfs.log
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
CFLAGS  = -g -Wall $(DEBUG) 
LDFLAGS = 
INSTALLDIR = /usr/sbin/ 

UNAME		:= $(shell uname)

# make sure we have PKG_CONFIG_PATH in environment
ifndef PKG_CONFIG_PATH
	PKG_CONFIG_PATH = /etc:/usr/local/lib/pkgconfig/:
endif
export PKG_CONFIG_PATH 

# set values for mntpnt and backend
ifndef PLFS_MNT 
	PLFS_MNT = /mnt/fuse
endif
ifndef PLFS_BACK
	PLFS_BACK = /net/scratch3/${USER}/plfs_back
endif


# removed direct_io bec can't exec files in PLFS if it is set
PLFS_SHARED_ARGS = -plfs_backend=$(PLFS_BACK) -plfs_subdirs=32 \
				   -plfs_synconclose=1
ifeq ($(UNAME), Darwin)
	# also google for -oauto_xattr and noapplespecial and noappledouble
	# -omodules=volicon -oiconpath=../../mac/plfs.icns
	UMOUNT    = umount $(PLFS_MNT)
	PLFS_ARGS = -o volname=PLFS $(PLFS_SHARED_ARGS) -plfs_bufferindex=0
else
	UMOUNT    = fusermount -u $(PLFS_MNT)
	PLFS_ARGS = $(PLFS_SHARED_ARGS) 
endif

CFLAGS  += `pkg-config fuse --cflags`
LDFLAGS += `pkg-config fuse --libs`

all: plfs.o 
#all: $(TARGET)

$(TARGET): main.o $(OBJS) 
	@ # this next line builds it all at once from all source
	@ # it's a teeny bit better for building from clean
	@ # but much worse if just a single file has changed
	@ #g++ $(CFLAGS) -o $(TARGET) $(SRCS) `pkg-config fuse --cflags --libs`
	@ # this next line just does the link from each individual object
	g++ $(LDFLAGS) -o $(TARGET) main.o $(OBJS) 

plfs_map: plfs_map.o $(OBJS)
	g++ $(LDFLAGS) -o $@ plfs_map.o $(OBJS)

%.o: %.C $(HDRS)
	g++ $(CFLAGS) -o $@ -c $<

%.o : %.cpp $(HDRS)
	g++ $(CFLAGS) -o $@ -c $<
 
clean:
	/bin/rm -f $(TARGET) plfs_map *.o

test:
	/bin/cat $(PLFS_MNT)/.plfsdebug
	#rm -f $(PLFS_MNT)/passwd
	/bin/cp /etc/passwd $(PLFS_MNT)
	ls -l $(PLFS_MNT)/passwd
	/usr/bin/diff -q /etc/passwd $(PLFS_MNT)
	/bin/cat $(PLFS_MNT)/.plfsdebug

stest:
	@grep -a Host $(PLFS_MNT)/.plfsdebug

$(PLFS_MNT):
	/bin/mkdir -p $(PLFS_MNT)

#$(PLFS_BACK):
#	/bin/mkdir -p $(PLFS_BACK)

install: $(TARGET)
	/bin/cp ./$(TARGET) $(INSTALLDIR)

mount: $(TARGET) $(PLFS_MNT) 
	./$(TARGET) -d $(PLFS_MNT) $(PLFS_ARGS)

smount: $(TARGET) $(PLFS_MNT) 
	./$(TARGET) $(PLFS_MNT) $(PLFS_ARGS)

lmount: $(TARGET) $(PLFS_MNT)
	/bin/rm -f $(LOG)
	./$(TARGET) -d $(PLFS_MNT) $(PLFS_ARGS) &> $(LOG) &

umount:
	$(UMOUNT)
# DO NOT DELETE
