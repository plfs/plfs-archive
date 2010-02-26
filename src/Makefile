LIBRARY    = libplfs.a
SRCS       = $(wildcard *.cpp) 
HDRS       = $(wildcard *.h)
OBJS       = $(addsuffix .o, $(basename $(SRCS) ) ) 
HADOOP_CXXFLAGS	= -I/home/milop/hadoop/hadoop-0.20.1/src/c++/libhdfs/ -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux
HADOOP_LDFLAGS = -L/home/milop/hadoop/hadoop-0.20.1/src/c++/install/lib -L${JAVA_HOME}/jre/lib/amd64/server/ -lhdfs -ljvm
# put -DNDEBUG in to turn off assertions, this can make a big difference
# as many of the asserts are doing stats
# put -DNUTIL in to turn off timing in Util
# add -DPLFS_TIMES to turn on timing in plfs
# add -DINDEX_CONTAINS_TIMESTAMPS to allow trace vizualization
# add -DCOUNT_SKIPS to maintain a count of sequential / non-sequential IO 
# add -DVERBOSE to get verbose output
DEBUG   =  -DPLFS_TIMES -DNUTIL -DINDEX_CONTAINS_TIMESTAMPS \
	      -DCOUNT_SKIPS -DVERBOSE
CXXFLAGS  = -g -Wall $(DEBUG) -D_FILE_OFFSET_BITS=64 $(HADOOP_CXXFLAGS)
LDFLAGS = -lpthread $(HADOOP_LDFLAGS)



UNAME		:= $(shell uname)

ifeq ($(UNAME), Darwin)
	CXXFLAGS += -D__FreeBSD__=10
endif

all: $(LIBRARY)

$(LIBRARY): $(OBJS) 
	ar rcs $@ $(OBJS) 

clean:
	/bin/rm -f $(LIBRARY) *.o
