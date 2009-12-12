PLFS_INSTALL_DIR = ..

TARGET   = plfs
CXXFLAGS = -I$(PLFS_INSTALL_DIR) -DPLFS_TIMES -DCOUNT_SKIPS -g -Wall
LDFLAGS  = -L$(PLFS_INSTALL_DIR) -lplfs
LOG      = /tmp/plfs.log

# remove direct_io if you want to exec a PLFS file
# but it makes the performance horrible because writes are split into 4K
# as opposed to 128K with direct_io 
PLFS_SHARED_ARGS = -o direct_io -plfs_backend=$(PLFS_BACK) -plfs_subdirs=32 \
				   -plfs_synconclose=1
UNAME := $(shell uname)
ifeq ($(UNAME), Darwin)
	UMOUNT    = umount $(PLFS_MNT)
	# also google for -oauto_xattr and noapplespecial and noappledouble
	# -omodules=volicon -oiconpath=../../mac/plfs.icns
	PLFS_ARGS = -o volname=PLFS $(PLFS_SHARED_ARGS) -plfs_bufferindex=0
	CXXFLAGS += -D__FreeBSD__=10
else
	PLFS_ARGS = $(PLFS_SHARED_ARGS) 
	UMOUNT    = fusermount -u $(PLFS_MNT)
endif
CXXFLAGS += `pkg-config fuse --cflags`
LDFLAGS  += `pkg-config fuse --libs`

# make sure we have PKG_CONFIG_PATH in environment
ifndef PKG_CONFIG_PATH
    PKG_CONFIG_PATH = /etc:/usr/local/lib/pkgconfig/:
endif
export PKG_CONFIG_PATH

# set values for mntpnt and backend
ifndef PLFS_MNT
    PLFS_MNT = /mnt/plfs
endif
ifndef PLFS_BACK
    PLFS_BACK = /tmp/plfs.back
endif

all: $(TARGET)

$(TARGET): plfs_fuse.o main.o
	g++ $(LDFLAGS) -o $(TARGET) main.o plfs_fuse.o 

mount: $(TARGET) $(PLFS_MNT) $(PLFS_BACK)
	./$(TARGET) -d $(PLFS_MNT) $(PLFS_ARGS)

smount: $(TARGET) $(PLFS_MNT) 
	./$(TARGET) $(PLFS_MNT) $(PLFS_ARGS)

lmount: $(TARGET) $(PLFS_MNT)
	/bin/rm -f $(LOG)
	./$(TARGET) -d $(PLFS_MNT) $(PLFS_ARGS) &> $(LOG) &

umount:
	$(UMOUNT)

clean:
	\rm -f $(TARGET) *.o

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

$(PLFS_BACK):
	/bin/mkdir -p $(PLFS_BACK)
# DO NOT DELETE
