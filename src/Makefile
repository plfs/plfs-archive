LIBRARY    = libplfs.a
SLIB       = libplfs.so
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
CXXFLAGS  = -g -Wall $(DEBUG) -D_FILE_OFFSET_BITS=64
LDFLAGS = 

UNAME		:= $(shell uname)

ifeq ($(UNAME), Darwin)
	CXXFLAGS += -D__FreeBSD__=10
	AR = libtool -static -o
	SLIB = libplfs.1.dylib
else 
	AR = ar rcs 
endif

all: $(LIBRARY) $(SLIB)

$(SLIB): $(OBJS)
	gcc -dynamiclib -Wl,-headerpad_max_install_names,-undefined,dynamic_lookup,-compatibility_version,1.0,-current_version,1.0,-install_name,./$@ -o $@ *.o

$(LIBRARY): $(OBJS) 
	$(AR) $@ $(OBJS) 

clean:
	/bin/rm -f $(LIBRARY) $(SLIB) *.o
