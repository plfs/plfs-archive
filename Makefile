all:
	make -C src all
	make -C fuse all

clean:
	make -C src clean
	make -C fuse clean
