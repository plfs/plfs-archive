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
INSTALLDIR = /usr/sbin/ 

UNAME		:= $(shell uname)

ifeq ($(UNAME), Darwin)
	# also google for -oauto_xattr and noapplespecial and noappledouble
	# -omodules=volicon -oiconpath=../../mac/plfs.icns
	UMOUNT    = umount $(PLFS_MNT)
	PLFS_ARGS = -o volname=PLFS $(PLFS_SHARED_ARGS) -plfs_bufferindex=0
	CFLAGS += -D__FreeBSD__=10
else
	UMOUNT    = fusermount -u $(PLFS_MNT)
	PLFS_ARGS = $(PLFS_SHARED_ARGS) 
endif

all: $(LIBRARY)

$(LIBRARY): $(OBJS) 
	ar rcs $@ $(OBJS) 

%.o: %.C $(HDRS)
	g++ $(CFLAGS) -o $@ -c $<

%.o : %.cpp $(HDRS)
	g++ $(CFLAGS) -o $@ -c $<
 
clean:
	/bin/rm -f $(LIBRARY) plfs_map *.o

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

install: $(LIBRARY)
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
