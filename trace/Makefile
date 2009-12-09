PLFS_INSTALL_DIR = ..

TARGET   = plfs_map
CXXFLAGS = -I$(PLFS_INSTALL_DIR)
LDFLAGS  = -L$(PLFS_INSTALL_DIR) -lplfs

UNAME := $(shell uname)
ifeq ($(UNAME), Darwin)
	CXXFLAGS += -D__FreeBSD__=10
endif

all: $(TARGET)

clean:
	\rm -f $(TARGET)
