PLFS_INSTALL_DIR = ..

TARGET   = plfs
CXXFLAGS = -I$(PLFS_INSTALL_DIR)
LDFLAGS  = -L$(PLFS_INSTALL_DIR) -lplfs

UNAME := $(shell uname)
ifeq ($(UNAME), Darwin)
	CXXFLAGS += -D__FreeBSD__=10
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
    PLFS_MNT = /mnt/fuse
endif
ifndef PLFS_BACK
    PLFS_BACK = /net/scratch3/${USER}/plfs_back
endif


all: $(TARGET)

clean:
	\rm -f $(TARGET)
