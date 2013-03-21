#CC=icpc
CC=g++

PLFS_PATH = /opt/plfs/2.0.1

# Sierra is odd and doesn't like xstat and fxstat when running in parallel. 
# It also doesn't like unlocked functions or putchar and getchar. These are disabled by default.

# To enable the __xstat and __fxstat functions, -DXSTAT
# To enable _unlocked functions, -DUNLOCKED
# To enable getchar and putchar, -DPUTGETCHAR

OPTS = -g -fPIC -shared -I$(PLFS_PATH)/include -fvisibility=hidden -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -DUNLOCKED -DPUTGETCHAR #-DXSTAT
LINKOPTS = -g -fPIC -shared -fvisibility=hidden -Wl,-soname,libldplfs.so.1 -o libldplfs.so.1.0.1 -lpthread -ldl -lplfs -L$(PLFS_PATH)/lib -I$(PLFS_PATH)/include

all: libldplfs 

libldplfs:
	rm -Rf *.so* # clean up any symlinks
	$(CC) $(OPTS) -O3 -c ldplfs.cpp
	$(CC) $(LINKOPTS) -O3 ldplfs.o
	ln -s libldplfs.so.1.0.1 libldplfs.so.1
	ln -s libldplfs.so.1 libldplfs.so

debug:
	rm -Rf *.so* # clean up any symlinks
	$(CC) $(OPTS) -O0 -DDEBUG -c ldplfs.cpp
	$(CC) $(LINKOPTS) -O0 ldplfs.o
	ln -s libldplfs.so.1.0.1 libldplfs.so.1
	ln -s libldplfs.so.1 libldplfs.so

	
clean:
	rm -f *.o *.so *.so.*
