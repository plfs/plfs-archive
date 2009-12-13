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
