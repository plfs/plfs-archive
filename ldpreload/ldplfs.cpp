#include <plfs.h>

#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>

#include <string>
#include <map>
#include <set>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>

#ifndef STATIC
	#define FORWARD_DECLARE(ret,name,args) \
		ret (*__real_ ## name)args = NULL;
	#define MAP(func, ret) \
		if (!(__real_ ## func)) { \
			__real_ ## func = (ret) dlsym(RTLD_NEXT, #func); \
	 		if (!(__real_ ## func)) std::cerr  << "Failed to link symbol: " << #func << std::endl; \
		}
	#define FUNCTION_DECLARE(func) func
#else
	#define FORWARD_DECLARE(ret, name, args) \
		extern ret __real_ ## name args;
	#define MAP(func)
	#define FUNCTION_DECLARE(func) __wrap_ ## func
#endif

#ifdef DEBUG
	#define DEBUG_NESTING \
		static int nestlevel = 0;
	#define DEBUG_ENTER \
		std::cerr << std::string(nestlevel++, '\t') << "Enter " << __FUNCTION__ << '\n';
	#define DEBUG_EXIT \
		std::cerr << std::string(--nestlevel, '\t') << "Exit " << __FUNCTION__ << '\n';
#else
	#define DEBUG_NESTING
	#define DEBUG_ENTER
	#define DEBUG_EXIT
#endif

DEBUG_NESTING;

// Whether to force PLFS syncs... (might not need to with writes... deffo need to with reads?)
static int writesync = 0;
static int readsync = 1;
static int flatten_on_close = 0;

int devnull = -1;

struct plfs_file_t {
	Plfs_fd *fd;
	std::string *path;
	mode_t mode;
	plfs_file_t(): fd(NULL), path(NULL), mode(0) {}
};

typedef plfs_file_t plfs_file;

std::map<int, plfs_file*> plfs_files;

struct plfs_dir_t {
	std::string *path;
	std::set<std::string> *files;
	std::set<std::string>::iterator itr;
	int dirfd;
	plfs_dir_t(): path(NULL), files(NULL), itr(NULL), dirfd(0) {}
};

typedef plfs_dir_t plfs_dir;

std::map<DIR*, plfs_dir*> opendirs;

FORWARD_DECLARE(int, creat, (const char *path, mode_t mode));
FORWARD_DECLARE(int, open, (const char *path, int flags, ...));
FORWARD_DECLARE(int, close, (int fd));
FORWARD_DECLARE(ssize_t, write, (int fd, const void *buf, size_t count));
FORWARD_DECLARE(ssize_t, read, (int fd, void *buf, size_t count));
FORWARD_DECLARE(ssize_t, pread, (int fd, void *buf, size_t count, off_t offset));
FORWARD_DECLARE(ssize_t, pwrite, (int fd, const void *buf, size_t count, off_t offset));

FORWARD_DECLARE(int, creat64, (const char *path, mode_t mode));
FORWARD_DECLARE(int, open64, (const char *path, int flags, ...));
FORWARD_DECLARE(ssize_t, pread64, (int fd, void *buf, size_t count, off64_t offset));
FORWARD_DECLARE(ssize_t, pwrite64, (int fd, const void *buf, size_t count, off64_t offset));

FORWARD_DECLARE(FILE *, fopen, (const char *path, const char *mode));
FORWARD_DECLARE(FILE *, freopen, (const char *path, const char *mode, FILE *stream));
FORWARD_DECLARE(int, fclose, (FILE *fp));
FORWARD_DECLARE(size_t, fread, (void *ptr, size_t size, size_t nmemb, FILE *stream));
FORWARD_DECLARE(size_t, fwrite, (const void *ptr, size_t size, size_t nmemb, FILE *stream));
//FORWARD_DECLARE(ssize_t, getline, (char **lineptr, size_t *n, FILE *stream));

FORWARD_DECLARE(int, fgetc, (FILE *stream));
FORWARD_DECLARE(char *, fgets, (char *s, int size, FILE *stream));
FORWARD_DECLARE(int, getchar, (void));
FORWARD_DECLARE(int, ungetc, (int c, FILE *stream));

FORWARD_DECLARE(int, fputc, (int c, FILE *stream));
FORWARD_DECLARE(int, fputs, (const char *s, FILE *stream));
FORWARD_DECLARE(int, putchar, (int c));
FORWARD_DECLARE(int, puts, (const char *s));

FORWARD_DECLARE(int, fflush, (FILE *stream));

#ifdef UNLOCKED
#ifdef __USE_MISC
FORWARD_DECLARE(int, fgetc_unlocked, (FILE *stream));
FORWARD_DECLARE(int, fputc_unlocked, (int c, FILE *stream));
FORWARD_DECLARE(size_t, fread_unlocked, (void *ptr, size_t size, size_t nmemb, FILE *stream));
FORWARD_DECLARE(size_t, fwrite_unlocked, (const void *ptr, size_t size, size_t nmemb, FILE *stream));
FORWARD_DECLARE(int, fflush_unlocked, (FILE *stream));
#endif

#ifdef __USE_GNU
FORWARD_DECLARE(char *, fgets_unlocked, (char *s, int size, FILE *stream));
FORWARD_DECLARE(int, fputs_unlocked, (const char *s, FILE *stream));
#endif

#if defined __USE_POSIX || defined __USE_MISC
FORWARD_DECLARE(int, getchar_unlocked, (void));
//FORWARD_DECLARE(int, getc_unlocked, (FILE *stream));
FORWARD_DECLARE(int, putchar_unlocked, (int c));
FORWARD_DECLARE(int, puts_unlocked, (const char *s));
#endif
#endif

FORWARD_DECLARE(int, fscanf, (FILE *stream, const char *format, ...));

FORWARD_DECLARE(int, __fxstat, (int vers, int fd, struct stat *buf));
FORWARD_DECLARE(int, __lxstat, (int vers, const char *path, struct stat *buf));
FORWARD_DECLARE(int, __xstat, (int vers, const char *path, struct stat *buf));

FORWARD_DECLARE(int, __fxstat64, (int vers, int fd, struct stat64 *buf));
FORWARD_DECLARE(int, __lxstat64, (int vers, const char *path, struct stat64 *buf));
FORWARD_DECLARE(int, __xstat64, (int vers, const char *path, struct stat64 *buf));

FORWARD_DECLARE(ssize_t, lgetxattr,(const char *path, const char *name, void *value, size_t size));
FORWARD_DECLARE(ssize_t, getxattr,(const char *path, const char *name, void *value, size_t size));
FORWARD_DECLARE(ssize_t, fgetxattr,(int fd, const char *name, void *value, size_t size));


FORWARD_DECLARE(struct dirent *, readdir, (DIR *dirp));
FORWARD_DECLARE(DIR *, opendir, (const char *name));
FORWARD_DECLARE(int, closedir, (DIR *dirp));
FORWARD_DECLARE(void, seekdir, (DIR *dirp, long offset));
FORWARD_DECLARE(void, rewinddir, (DIR *dirp));
FORWARD_DECLARE(long, telldir, (DIR *dirp));


FORWARD_DECLARE(ssize_t, readlink, (const char *path, char *buf, size_t bufsiz));
FORWARD_DECLARE(int, link, (const char *oldpath, const char *newpath));
FORWARD_DECLARE(int, symlink, (const char *oldpath, const char *newpath));
FORWARD_DECLARE(int, unlink, (const char *pathname));


FORWARD_DECLARE(int, chmod, (const char *path, mode_t mode));
FORWARD_DECLARE(int, fchmod, (int fd, mode_t mode));
FORWARD_DECLARE(int, fchmodat, (int dirfd, const char *pathname, mode_t mode, int flags));
FORWARD_DECLARE(int, chown, (const char *path, uid_t owner, gid_t group));
FORWARD_DECLARE(int, lchown, (const char *path, uid_t owner, gid_t group));
FORWARD_DECLARE(int, fchown, (int fd, uid_t owner, gid_t group));

FORWARD_DECLARE(int, rename, (const char *oldpath, const char *newpath));

FORWARD_DECLARE(int, statvfs, (const char *path, struct statvfs *buf));
FORWARD_DECLARE(int, fstatvfs, (int fd, struct statvfs *buf));

FORWARD_DECLARE(int, truncate, (const char *path, off_t length));
FORWARD_DECLARE(int, ftruncate, (int fd, off_t length));
FORWARD_DECLARE(int, truncate64, (const char *path, off64_t length));
FORWARD_DECLARE(int, ftruncate64, (int fd, off64_t length));

FORWARD_DECLARE(void, sync, (void));
FORWARD_DECLARE(void, syncfs, (int fd));
FORWARD_DECLARE(int, fsync, (int fd));
FORWARD_DECLARE(int, fdatasync, (int fd));

FORWARD_DECLARE(int, mkdir, (const char *pathname, mode_t mode));
FORWARD_DECLARE(int, rmdir, (const char *pathname));

FORWARD_DECLARE(int, dup, (int oldfd));
FORWARD_DECLARE(int, dup2, (int oldfd, int newfd));
FORWARD_DECLARE(int, dup3, (int oldfd, int newfd, int flags));

FORWARD_DECLARE(FILE *, tmpfile, (void));


std::vector<std::string> mount_points;

void loadMounts() {
	DEBUG_ENTER;
	
	MAP(open, int (*)(const char*, int, ...));
	MAP(read,ssize_t (*)(int, void*, size_t));
	MAP(close,int (*)(int));
	
	std::vector<std::string> possible_files;
	if (getenv("PLFSRC")) {
		std::string env_file = getenv("PLFSRC");
		possible_files.push_back(env_file);
	}
	if (getenv("HOME")) {
		std::string home_file = getenv("HOME");
		home_file.append("/.plfsrc");
		possible_files.push_back(home_file);
	}
	possible_files.push_back("/etc/plfsrc");
	
	for (std::vector<std::string>::iterator itr = possible_files.begin(); itr != possible_files.end(); itr++) {
		std::string contents;
		char buf[1024];

		int fd = __real_open(itr->c_str(), O_RDONLY);
		
		if (fd < 0) continue;
		
		int bytes = 0;
		while ((bytes = __real_read(fd, buf, 1024)) > 0) {
			contents.append(buf, bytes);
		}
		
		__real_close(fd);
		
		std::stringstream ss (contents);
		
		std::string mp ("mount_point");
		std::string whitespace (" \t\n");
		
		while (ss.good()) {
			char line[1024];
			ss.getline(line, 1024);
			
			if (std::string(line).find(mp) != std::string::npos) {
				std::string tmp = std::string(line).substr(std::string(line).find(mp) + mp.size());
				mount_points.push_back(tmp.substr(tmp.find_first_not_of(whitespace), tmp.find_last_not_of(whitespace) - tmp.find_first_not_of(whitespace) + 1));
			}
		}
	}
	
	if (mount_points.size() == 0) {
		std::cerr << "There were no mount points defined." << std::endl;
	}
	
	DEBUG_EXIT;
}

int is_plfs_path(const char *path) {
	DEBUG_ENTER;
	
	if (mount_points.size() == 0) loadMounts();
	
	std::string p (path);
	
	int ret = 0;
	
	for (std::vector<std::string>::iterator itr = mount_points.begin(); itr != mount_points.end(); itr++) {
		if (p.find(*itr) != std::string::npos) { 
			ret = 1;
			break;
		}
	}
	
	DEBUG_EXIT;
	return ret;
}

int getflags(const char *mode) {
	std::string stmode = std::string(mode);
	
	if (stmode.compare("r") == 0) return O_RDONLY;
	else if (stmode.compare("r+") == 0) return O_RDWR;
	else if (stmode.compare("w") == 0) return O_WRONLY | O_TRUNC | O_CREAT;
	else if (stmode.compare("w+") == 0) return O_RDWR | O_TRUNC | O_CREAT;
	else if (stmode.compare("a") == 0) return O_WRONLY | O_CREAT | O_APPEND;
	else if (stmode.compare("a+") == 0) return O_RDWR | O_CREAT | O_APPEND;
	else return 0;
}

__attribute__((destructor)) void my_destructor() {
	if (devnull != -1) {
		MAP(close,int (*)(int));
		__real_close(devnull);
	}
}

char *resolvefd(int fd) {
	char *path = (char *) malloc(sizeof(char) * 1024);

	char *tmp = (char *) malloc(sizeof(char) * 1024);

	sprintf(tmp, "/proc/self/fd/%d", fd);

	ssize_t len = readlink(tmp, path, 1024);

	path = (char *) realloc(path, len + 1);
	path[len] = '\0';

	free(tmp);

	return path;
}

/* given a relative path, calculate complete path assuming current directory */
char *resolvePath(const char *p) {
	char *ret;

	std::string path (p);

	// if first character isn't "/", prepend current working dir
	if (path[0] != '/') {
		char *cwd = get_current_dir_name();
		path = std::string(cwd) + "/" + path;
		free(cwd);
	}

	if (path.find("/./") != std::string::npos) {
		int stop = path.length()-2;
		// iterate over string... if 3 characters are /./, call replace and replace them with /
		for (int i=0; i < stop; i++) {
			if (path.substr(i,3).compare("/./") == 0) {
				path.replace(i,3,"/");
				stop = path.length()-2;
				i--;
			}
		}
	}       
			
	if (path.find("//") != std::string::npos) {
		int stop = path.length()-1;
		for (int i=0; i < stop; i++) {
			if (path.substr(i,2).compare("//") == 0) {
				path.replace(i,2,"/");
				stop = path.length()-1;
				i--;
			}
		}
	}

	if (path.find("/../") != std::string::npos) {
		// this is the difficult one....
		int stop = path.length()-3;
		int lastslash = 0;
		for (int i=0; i < stop; i++) {
			if (path.substr(i,4).compare("/../") == 0) {
				if (i == 0) {
					path.replace(0,3,"");
					stop = path.length()-3;
					i--;
				} else {
					size_t l = path.find_last_of('/', i-1);
					path.replace(l, i-l+3, "");
					stop = path.length()-3;
					i = l-1;
					// find the previous /
				}
			}
		}
	}

	ret = (char *) malloc((path.length()+1) * sizeof(char));
	strcpy(ret, path.c_str());
	return ret;
}

int isDuplicated(int fd) {
	plfs_file *tmp = plfs_files.find(fd)->second;
	for (std::map<int,plfs_file*>::iterator itr = plfs_files.begin(); itr != plfs_files.end(); itr++) {
		if ((itr->first != fd) && (itr->second == tmp)) return 1; 
	}
	return 0;
}

#pragma GCC visibility push(default)

#ifdef __cplusplus
extern "C" {
#endif

int FUNCTION_DECLARE(dup)(int oldfd) {
	MAP(dup, int (*)(int));

	int ret = __real_dup(oldfd);

	if (plfs_files.find(oldfd) != plfs_files.end()) {
		DEBUG_ENTER;

		plfs_file *tmp = plfs_files.find(oldfd)->second;
		plfs_files.insert(std::pair<int, plfs_file *>(ret, tmp));

		DEBUG_EXIT;
	}
	
	return ret;
}

int FUNCTION_DECLARE(dup2)(int oldfd, int newfd) {
	MAP(dup2, int (*)(int, int));

	int ret = __real_dup2(oldfd, newfd);

	if (plfs_files.find(oldfd) != plfs_files.end()) {
		DEBUG_ENTER;

		plfs_file *tmp = plfs_files.find(oldfd)->second;
		
		plfs_files.insert(std::pair<int, plfs_file *>(newfd, tmp));

		DEBUG_EXIT;
	}

	return ret;
}

int FUNCTION_DECLARE(dup3)(int oldfd, int newfd, int flags) {
	MAP(dup3, int (*)(int, int, int));

	int ret = __real_dup3(oldfd, newfd, flags);
	
	if (plfs_files.find(oldfd) != plfs_files.end()) {
		DEBUG_ENTER;

		plfs_file *tmp = plfs_files.find(oldfd)->second;
		plfs_files.insert(std::pair<int, plfs_file *>(newfd, tmp));

		DEBUG_EXIT;
	}

	return ret;
}

/*
 * Need to catch all these fuckers too. and all _unlocked.
 *
 * fopen, freopen, fclose, fread, **getline, fwrite, fputs, fputc, putc, puts, putchar, fgets, fgetc, getc, getchar, freopen, fflush, fscanf, **fprintf, **fgetpos, **fsetpos, **feof
 *
 */

FILE * FUNCTION_DECLARE(fopen)(const char *path, const char *mode) {
	MAP(fopen, FILE *(*)(const char *, const char *));
	MAP(tmpfile, FILE *(*)(void));
	
	int flags = getflags(mode);
	
	FILE *ret;

	char *cpath = resolvePath(path);

	if (is_plfs_path(cpath)) {
		DEBUG_ENTER;
		
		//Plfs_open_opt opts;
		//opts.index_stream = NULL;
		//opts.pinter = PLFS_MPIIO;

		plfs_file *tmp = new plfs_file();
		
		mode_t fmode = umask(S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
		umask(fmode);
		
		if ((errno = plfs_open(&(tmp->fd), cpath, flags, getpid(), fmode, NULL)) != 0) {
			errno = -errno;
			ret = NULL;
			delete tmp;
		} else {
			//ret = __real_fopen("/dev/random", mode);
			ret = __real_tmpfile();

			tmp->path = new std::string(cpath);
			tmp->mode = flags;
		
			plfs_files.insert(std::pair<int, plfs_file *>(fileno(ret), tmp));
		}

		DEBUG_EXIT;
	} else {
		ret = __real_fopen(path, mode);
	}

	free(cpath);
	
	return ret;
}

FILE * FUNCTION_DECLARE(freopen)(const char *path, const char *mode, FILE *stream) {
	MAP(freopen, FILE *(*)(const char *, const char *, FILE *));

	int flags = getflags(mode);
	
	FILE *ret;
	
	char *cpath = resolvePath(path);
	
	if (is_plfs_path(cpath)) {
		DEBUG_ENTER;

		//Plfs_open_opt opts;
		//opts.index_stream = NULL;
		//opts.pinter = PLFS_MPIIO;
	
		if (stream != NULL) fclose(stream);
		
		plfs_file *tmp = new plfs_file();
		
		mode_t fmode = umask(S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
		umask(fmode);
		
		if ((errno = plfs_open(&(tmp->fd), cpath, flags, getpid(), fmode, NULL)) != 0) {
			errno = -errno; // flip errno
			ret = NULL;
			delete tmp;
		} else {
			ret = __real_freopen(tmpnam(NULL), mode, stream);
			//ret = __real_tmpfile();

			tmp->path = new std::string(cpath);
			tmp->mode = flags;
		
			plfs_files.insert(std::pair<int, plfs_file *>(fileno(ret), tmp));
		}
		DEBUG_EXIT;
	} else {
		ret = __real_freopen(path, mode, stream);
	}
	
	free(cpath);
	
	return ret;
}

int FUNCTION_DECLARE(fclose)(FILE *fp) {
	MAP(fclose, int (*)(FILE *));

	int ret = 0;
	
	if (plfs_files.find(fileno(fp)) != plfs_files.end()) {
		DEBUG_ENTER;

		if (!isDuplicated(fileno(fp))) {
			if (flatten_on_close) plfs_flatten_index(plfs_files.find(fileno(fp))->second->fd, plfs_files.find(fileno(fp))->second->path->c_str());
			plfs_close(plfs_files.find(fileno(fp))->second->fd, getpid(), getuid(), plfs_files.find(fileno(fp))->second->mode, NULL);
			delete plfs_files.find(fileno(fp))->second->path;
			delete plfs_files.find(fileno(fp))->second;
		}
		plfs_files.erase(fileno(fp));

		DEBUG_EXIT;
	}

	ret = __real_fclose(fp);

	return ret;
}

size_t FUNCTION_DECLARE(fread)(void *ptr, size_t size, size_t nmemb, FILE *stream) {

	
	MAP(fread, size_t (*)(void *, size_t, size_t, FILE *));
	
	size_t ret;
	
	if (plfs_files.find(fileno(stream)) != plfs_files.end()) {
		DEBUG_ENTER;
		long offset = ftell(stream);
		ret = plfs_read(plfs_files.find(fileno(stream))->second->fd, (char *) ptr, size * nmemb, offset);

		fseek(stream, ret, SEEK_CUR);

		if (readsync) plfs_sync(plfs_files.find(fileno(stream))->second->fd, getpid());
		DEBUG_EXIT;
	} else {
		ret = __real_fread(ptr, size, nmemb, stream);
	}

	return ret;
}

size_t FUNCTION_DECLARE(fwrite)(const void *ptr, size_t size, size_t nmemb, FILE *stream) {
	MAP(fwrite, size_t (*)(const void *, size_t, size_t, FILE *));
	
	size_t ret;
	
	if (plfs_files.find(fileno(stream)) != plfs_files.end()) {
		DEBUG_ENTER;

		long offset = ftell(stream);
		ret = plfs_write(plfs_files.find(fileno(stream))->second->fd, (const char *) ptr, size * nmemb, offset, getpid());

		fseek(stream, ret, SEEK_CUR);

		if (writesync) plfs_sync(plfs_files.find(fileno(stream))->second->fd, getpid());

		DEBUG_EXIT;
	} else {
		ret = __real_fwrite(ptr, size, nmemb, stream);
	}
	
	return ret;
}



int FUNCTION_DECLARE(fscanf)(FILE *stream, const char *format, ...) {
	MAP(fscanf, int (*)(FILE *, const char *, ...));
	
	int ret;
	
	va_list argp;
	va_start(argp, format);
	
	if (plfs_files.find(fileno(stream)) != plfs_files.end()) {
		DEBUG_ENTER;

		char buffer1[BUFSIZ];
		char buffer2[BUFSIZ];
		
		long offset = ftell(stream);
		
		ssize_t bytes = plfs_read(plfs_files.find(fileno(stream))->second->fd, (char *) buffer1, BUFSIZ, offset);
		
		ret = vsscanf(buffer1, format, argp);
		
		int n = vsprintf(buffer2, format, argp); // Work out how many characters are read to match input.
		fseek(stream, n, SEEK_CUR);

		DEBUG_EXIT;
	} else {

		ret = vfscanf(stream, format, argp);

	}
	va_end(argp);
	
	return ret;
}

int FUNCTION_DECLARE(fgetc)(FILE *stream) {
	MAP(fgetc, int (*)(FILE *));
	
	int ret;
	
	if (plfs_files.find(fileno(stream)) != plfs_files.end()) {
		DEBUG_ENTER;

		char buf;
		
		long offset = ftell(stream);
		ssize_t bytes = plfs_read(plfs_files.find(fileno(stream))->second->fd, &buf, 1, offset);

		fseek(stream, bytes, SEEK_CUR);

		if (readsync) plfs_sync(plfs_files.find(fileno(stream))->second->fd, getpid());
		
		ret = buf;

		DEBUG_EXIT;
	} else {
		ret = __real_fgetc(stream);
	}

	return ret;
}

char * FUNCTION_DECLARE(fgets)(char *s, int size, FILE *stream) {
	MAP(fgets, char *(*)(char *, int, FILE *));
	
	char *ret;
	
	if (plfs_files.find(fileno(stream)) != plfs_files.end()) {
		DEBUG_ENTER;
		
		long offset = ftell(stream);
		ssize_t bytes = plfs_read(plfs_files.find(fileno(stream))->second->fd, s, size - 1, offset);
		
		char *eol = strchr(s, '\n');
		if (eol == NULL) {
			fseek(stream, bytes, SEEK_CUR);
		} else {
			eol[1] = '\0';
			fseek(stream, eol - s, SEEK_CUR);
		}

		if (readsync) plfs_sync(plfs_files.find(fileno(stream))->second->fd, getpid());
		
		ret = s;
		
		DEBUG_EXIT;
	} else {
		ret = __real_fgets(s, size, stream);
	}

	return ret;
}

int FUNCTION_DECLARE(getc)(FILE *stream) {
	int ret = fgetc(stream);
	return ret;
}

#ifdef PUTGETCHAR
int FUNCTION_DECLARE(getchar)(void) {
	MAP(getchar, int (*)(void));
	
	int ret;
	
	if (plfs_files.find(fileno(stdin)) != plfs_files.end()) {
		DEBUG_ENTER;
		
		ret = fgetc(stdin);
		
		DEBUG_EXIT;
	} else {
		ret = __real_getchar();
	}

	return ret;
}

int FUNCTION_DECLARE(putchar)(int c) {
	MAP(putchar, int (*)(int));
	
	int ret;
	
	if (plfs_files.find(fileno(stdout)) != plfs_files.end()) {
		DEBUG_ENTER;
		
		ret = fputc(c, stdout);
		
		DEBUG_EXIT;
	} else {
		ret = __real_putchar(c);
	}

	return ret;
}
#endif

int FUNCTION_DECLARE(ungetc)(int c, FILE *stream) {
	MAP(ungetc, int (*)(int, FILE *));
	
	int ret;
	
	if (plfs_files.find(fileno(stream)) != plfs_files.end()) {
		DEBUG_ENTER;
		
		ret = c;
		fseek(stream, -1, SEEK_CUR);
		
		DEBUG_EXIT;
	} else {
		ret = __real_ungetc(c, stream);
	}

	return ret;
}

int FUNCTION_DECLARE(fputc)(int c, FILE *stream) {
	MAP(fputc, int (*)(int, FILE *));
	
	int ret;
	
	if (plfs_files.find(fileno(stream)) != plfs_files.end()) {
		DEBUG_ENTER;
		
		long offset = ftell(stream);
		ssize_t bytes = plfs_write(plfs_files.find(fileno(stream))->second->fd, (const char *) &c, 1, offset, getpid());

		fseek(stream, bytes, SEEK_CUR);
		
		if (writesync) plfs_sync(plfs_files.find(fileno(stream))->second->fd, getpid());
		
		DEBUG_EXIT;
	} else {
		ret = __real_fputc(c, stream);
	}

	return ret;
}

int FUNCTION_DECLARE(fputs)(const char *s, FILE *stream) {
	MAP(fputs, int (*)(const char *, FILE *));
	
	int ret = -1;
	
	if (plfs_files.find(fileno(stream)) != plfs_files.end()) {
		DEBUG_ENTER;
		
		long offset = ftell(stream);
		ssize_t bytes = plfs_write(plfs_files.find(fileno(stream))->second->fd, s, strlen(s), offset, getpid());

		fseek(stream, bytes, SEEK_CUR);
		
		if (writesync) plfs_sync(plfs_files.find(fileno(stream))->second->fd, getpid());
		
		ret = bytes;
		
		DEBUG_EXIT;
	} else {
		ret = __real_fputs(s, stream);
	}

	return ret;
}

int FUNCTION_DECLARE(putc)(int c, FILE *stream) {
	int ret = fputc(c, stream);
	return ret;
}

int FUNCTION_DECLARE(puts)(const char *s) {
	MAP(puts, int (*)(const char *));
	
	int ret;
	
	if (plfs_files.find(fileno(stdout)) != plfs_files.end()) {
		DEBUG_ENTER;
		
		ret = fputs(s, stdout);
		
		DEBUG_EXIT;
	} else {
		ret = __real_puts(s);
	}

	return ret;
}

int FUNCTION_DECLARE(fflush)(FILE *stream) {
	MAP(fflush, int (*)(FILE *));
	
	int ret;
	
	if (plfs_files.find(fileno(stream)) != plfs_files.end()) {
		DEBUG_ENTER;
		
		ret = plfs_sync(plfs_files.find(fileno(stream))->second->fd, getpid());
		
		DEBUG_EXIT;
	} else {
		ret = __real_fflush(stream);
	}
	
	return ret;
}

/* unlocked operations! */

#ifdef UNLOCKED
#ifdef __USE_MISC
int FUNCTION_DECLARE(fgetc_unlocked)(FILE *stream) {
	MAP(fgetc_unlocked, int (*)(FILE *));
	
	int ret;
	
	if (plfs_files.find(fileno(stream)) != plfs_files.end()) {
		DEBUG_ENTER;
		
		char buf;
		
		long offset = ftell(stream);
		ssize_t bytes = plfs_read(plfs_files.find(fileno(stream))->second->fd, &buf, 1, offset);

		fseek(stream, bytes, SEEK_CUR);

		if (readsync) plfs_sync(plfs_files.find(fileno(stream))->second->fd, getpid());
		
		ret = buf;
		
		DEBUG_EXIT;
	} else {
		ret = __real_fgetc_unlocked(stream);
	}

	return ret;
}

int FUNCTION_DECLARE(fputc_unlocked)(int c, FILE *stream) {
	MAP(fputc_unlocked, int (*)(int, FILE *));
	
	int ret;
	
	if (plfs_files.find(fileno(stream)) != plfs_files.end()) {
		DEBUG_ENTER;
		
		long offset = ftell(stream);
		ssize_t bytes = plfs_write(plfs_files.find(fileno(stream))->second->fd, (const char *) &c, 1, offset, getpid());

		fseek(stream, bytes, SEEK_CUR);
		
		if (writesync) plfs_sync(plfs_files.find(fileno(stream))->second->fd, getpid());
		
		DEBUG_EXIT;
	} else {
		ret = __real_fputc_unlocked(c, stream);
	}

	return ret;
}

int FUNCTION_DECLARE(putc_unlocked)(int c, FILE *stream) {
	int ret = fputc_unlocked(c, stream);
	return ret;
}

int FUNCTION_DECLARE(fflush_unlocked)(FILE *stream) {
	MAP(fflush_unlocked, int (*)(FILE *));
	
	int ret;
	
	if (plfs_files.find(fileno(stream)) != plfs_files.end()) {
		DEBUG_ENTER;
		
		ret = plfs_sync(plfs_files.find(fileno(stream))->second->fd, getpid());
		
		DEBUG_EXIT;
	} else {
		ret = __real_fflush_unlocked(stream);
	}

	return ret;
}

size_t FUNCTION_DECLARE(fread_unlocked)(void *ptr, size_t size, size_t nmemb, FILE *stream) {
	MAP(fread_unlocked, size_t (*)(void *, size_t, size_t, FILE *));
	
	size_t ret;
	
	if (plfs_files.find(fileno(stream)) != plfs_files.end()) {
		DEBUG_ENTER;
		
		long offset = ftell(stream);
		ret = plfs_read(plfs_files.find(fileno(stream))->second->fd, (char *) ptr, size * nmemb, offset);

		fseek(stream, ret, SEEK_CUR);

		if (readsync) plfs_sync(plfs_files.find(fileno(stream))->second->fd, getpid());
		
		DEBUG_EXIT;
	} else {
		ret = __real_fread_unlocked(ptr, size, nmemb, stream);
	}

	return ret;
}

size_t FUNCTION_DECLARE(fwrite_unlocked)(const void *ptr, size_t size, size_t nmemb, FILE *stream) {
	MAP(fwrite_unlocked, size_t (*)(const void *, size_t, size_t, FILE *));
	
	size_t ret;
	
	if (plfs_files.find(fileno(stream)) != plfs_files.end()) {
		DEBUG_ENTER;
		
		long offset = ftell(stream);
		ret = plfs_write(plfs_files.find(fileno(stream))->second->fd, (const char *) ptr, size * nmemb, offset, getpid());

		fseek(stream, ret, SEEK_CUR);

		if (writesync) plfs_sync(plfs_files.find(fileno(stream))->second->fd, getpid());
		
		DEBUG_EXIT;
	} else {
		ret = __real_fwrite_unlocked(ptr, size, nmemb, stream);
	}

	return ret;
}
#endif

#ifdef __USE_GNU
int FUNCTION_DECLARE(fputs_unlocked)(const char *s, FILE *stream) {
	MAP(fputs_unlocked, int (*)(const char *, FILE *));
	
	int ret = -1;
	
	if (plfs_files.find(fileno(stream)) != plfs_files.end()) {
		DEBUG_ENTER;
		
		long offset = ftell(stream);
		ssize_t bytes = plfs_write(plfs_files.find(fileno(stream))->second->fd, s, strlen(s), offset, getpid());

		fseek(stream, bytes, SEEK_CUR);
		
		if (writesync) plfs_sync(plfs_files.find(fileno(stream))->second->fd, getpid());
		
		ret = bytes;
		
		DEBUG_EXIT;
	} else {
		ret = __real_fputs_unlocked(s, stream);
	}
	

	return ret;
}

char * FUNCTION_DECLARE(fgets_unlocked)(char *s, int size, FILE *stream) {
	MAP(fgets_unlocked, char *(*)(char *, int, FILE *));
	
	char *ret;
	
	if (plfs_files.find(fileno(stream)) != plfs_files.end()) {
		DEBUG_ENTER;
		
		long offset = ftell(stream);
		ssize_t bytes = plfs_read(plfs_files.find(fileno(stream))->second->fd, s, size - 1, offset);
		
		char *eol = strchr(s, '\n');
		if (eol == NULL) {
			fseek(stream, bytes, SEEK_CUR);
		} else {
			eol[1] = '\0';
			fseek(stream, eol - s, SEEK_CUR);
		}

		if (readsync) plfs_sync(plfs_files.find(fileno(stream))->second->fd, getpid());
		
		ret = s;
		
		DEBUG_EXIT;
	} else {
		ret = __real_fgets_unlocked(s, size, stream);
	}

	return ret;
}
#endif

#if defined __USE_POSIX || defined __USE_MISC
int FUNCTION_DECLARE(getc_unlocked)(FILE *stream) {
	int ret = fgetc_unlocked(stream);
	return ret;
}

int FUNCTION_DECLARE(getchar_unlocked)(void) {
	MAP(getchar_unlocked, int (*)(void));
	
	int ret;
	
	if (plfs_files.find(fileno(stdin)) != plfs_files.end()) {
		DEBUG_ENTER;
		
		ret = fgetc_unlocked(stdin);
		
		DEBUG_EXIT;
	} else {
		ret = __real_getchar_unlocked();
	}

	return ret;
}

int FUNCTION_DECLARE(putchar_unlocked)(int c) {
	MAP(putchar_unlocked, int (*)(int));
	
	int ret;
	
	if (plfs_files.find(fileno(stdout)) != plfs_files.end()) {
		DEBUG_ENTER;
		
		ret = fputc_unlocked(c, stdout);
		
		DEBUG_EXIT;
	} else {
		ret = __real_putchar_unlocked(c);
	}

	return ret;
}

int FUNCTION_DECLARE(puts_unlocked)(const char *s) {
	MAP(puts_unlocked, int (*)(const char *));
	
	int ret;
	
	if (plfs_files.find(fileno(stdout)) != plfs_files.end()) {
		DEBUG_ENTER;
		
		ret = fputs_unlocked(s, stdout);
		
		DEBUG_EXIT;
	} else {
		ret = __real_puts_unlocked(s);
	}

	return ret;
}
#endif
#endif

/*
 * Operations on Directories.
 *
 * opendir, closedir, readdir, seekdir, telldir, rewinddir, rmdir, mkdir
 *
 */
//#if 0
DIR * FUNCTION_DECLARE(opendir)(const char *name) {
	MAP(opendir, DIR *(*)(const char*));
	
	DIR *ret;

	char *cpath = resolvePath(name);

	if (is_plfs_path(cpath)) {
		DEBUG_ENTER;
		
		ret = __real_opendir("/");
		
		plfs_dir *d = new plfs_dir();
		d->path = new std::string(cpath);
		
		d->files = new std::set<std::string>();
		plfs_readdir(cpath, (void *) d->files);
		
		d->itr = d->files->begin();
		
		d->dirfd = dirfd(ret);
		
		opendirs.insert(std::pair<DIR*,plfs_dir*>(ret,d));
		
		DEBUG_EXIT;
	} else {
		ret = __real_opendir(name);
	}

	free(cpath);
	
	return ret;
}

int FUNCTION_DECLARE(closedir)(DIR *dirp) {
	MAP(closedir, int (*)(DIR *dirp));
	
	int ret = 0;

	if (opendirs.find(dirp) != opendirs.end()) {
		DEBUG_ENTER;
		
		delete opendirs.find(dirp)->second->path;
		delete opendirs.find(dirp)->second;
		opendirs.erase(dirp);
		
		DEBUG_EXIT;
	}

	ret = __real_closedir(dirp);

	return ret;
}

struct dirent * FUNCTION_DECLARE(readdir)(DIR *dirp) {
	MAP(readdir,struct dirent *(*)(DIR*));

	struct dirent *ret;
	
	if (opendirs.find(dirp) != opendirs.end()) {
		DEBUG_ENTER;
		
		plfs_dir *d = opendirs.find(dirp)->second;
		
		if (d->itr == d->files->end()) { 
			ret = NULL;
		} else {
			struct dirent tmp;
			
			std::string fpath(*(d->path));
			fpath += "/";
			fpath += d->itr->c_str();
			
			struct stat stats;
			plfs_getattr(NULL, fpath.c_str(), &stats, 0);
			
			tmp.d_ino = stats.st_ino;

			sprintf(tmp.d_name, "%s", d->itr->c_str());
			
			ret = &tmp;
			
			d->itr++;
		}
		
		DEBUG_EXIT;
	} else {
		ret = __real_readdir(dirp);
	}
	
	return ret;
}

void FUNCTION_DECLARE(seekdir)(DIR *dirp, long offset) {
	MAP(seekdir, void (*)(DIR *, long));
	
	int ret = 0;
	
	if (opendirs.find(dirp) != opendirs.end()) {
		DEBUG_ENTER;
		
		plfs_dir *d = opendirs.find(dirp)->second;
		d->itr = d->files->begin();
		if (offset >= 0) 
			for (long i = 0; i < offset; i++, d->itr++);
			
		DEBUG_EXIT;
	} else {
		__real_seekdir(dirp, offset);
	}

	return;
}

void FUNCTION_DECLARE(rewinddir)(DIR *dirp) {
	MAP(rewinddir, void (*)(DIR *));
	
	int ret = 0;
	
	if (opendirs.find(dirp) != opendirs.end()) {
		DEBUG_ENTER;
		
		plfs_dir *d = opendirs.find(dirp)->second;
		d->itr = d->files->begin();
		
		DEBUG_EXIT;
	} else {
		__real_rewinddir(dirp);
	}

	return;
}

long FUNCTION_DECLARE(telldir)(DIR *dirp) {
	MAP(telldir, long (*)(DIR *));
	
	int ret = 0;
	
	if (opendirs.find(dirp) != opendirs.end()) {
		DEBUG_ENTER;
		
		plfs_dir *d = opendirs.find(dirp)->second;
		
		ret = std::distance(d->itr,d->files->begin());
		
		DEBUG_EXIT;
	} else {
		ret = __real_telldir(dirp);
	}
	
	
	return ret;
}

int FUNCTION_DECLARE(mkdir)(const char *pathname, mode_t mode) {
	MAP(mkdir, int (*)(const char *, mode_t));
	
	int ret = 0;
	
	char *cpath = resolvePath(pathname);
	
	if (is_plfs_path(cpath)) {
		DEBUG_ENTER;
		
		ret = plfs_mkdir(cpath, mode);
		
		DEBUG_EXIT;
	} else {
		ret = __real_mkdir(pathname, mode);
	}
	
	free(cpath);

	return ret;
}

int FUNCTION_DECLARE(rmdir)(const char *pathname) {
	MAP(rmdir, int (*)(const char *));
	
	int ret = 0;
	
	char *cpath = resolvePath(pathname);
	
	if (is_plfs_path(cpath)) {
		DEBUG_ENTER;
		
		ret = plfs_rmdir(cpath);
		
		DEBUG_EXIT;
	} else {
		ret = __real_rmdir(pathname);
	}
	
	free(cpath);

	return ret;
}

/*
 * Manipulating file access permissions and ownership
 *
 * chmod, chown, fchmod, fchown, lchown, fchmodat
 *
 */

int FUNCTION_DECLARE(chmod)(const char *path, mode_t mode) {
	MAP(chmod, int (*)(const char *, mode_t));
	
	int ret = 0;
	
	char *cpath = resolvePath(path);
	
	if (is_plfs_path(cpath)) {
		DEBUG_ENTER;
		
		ret = plfs_chmod(cpath, mode);
		
		DEBUG_EXIT;
	} else {
		ret = __real_chmod(path, mode);
	}
	
	free(cpath);

	return ret;
}

int FUNCTION_DECLARE(fchmod)(int fd, mode_t mode) {
	MAP(fchmod, int (*)(int, mode_t));
	
	int ret = 0;
	
	if (plfs_files.find(fd) != plfs_files.end()) {
		DEBUG_ENTER;
		
		ret = plfs_chmod(plfs_files.find(fd)->second->path->c_str(), mode);
		
		DEBUG_EXIT;
	} else {
		ret = __real_fchmod(fd, mode);
	}

	return ret;
}

int FUNCTION_DECLARE(fchmodat)(int dirfd, const char *path, mode_t mode, int flags) {
	MAP(fchmodat, int (*)(int, const char *, mode_t, int));
	
	/* NEED TO DEAL WITH AT_FDCWD and other things! openat too! and every other *at function! */
	
	int ret = 0;
	
	char *cpath = resolvePath(path);
	
	if (is_plfs_path(cpath)) {
		DEBUG_ENTER;
		
		ret = plfs_chmod(cpath, mode);
		
		DEBUG_EXIT;
	} else {
		ret = __real_fchmodat(dirfd, path, mode, flags);
	}
	
	free(cpath);
	
	return ret;
}

int FUNCTION_DECLARE(chown)(const char *path, uid_t owner, gid_t group) {
	MAP(chown, int (*)(const char *, uid_t, gid_t));
	
	int ret = 0;
	
	char *cpath = resolvePath(path);
	
	if (is_plfs_path(cpath)) {
		DEBUG_ENTER;
		
		ret = plfs_chown(cpath, owner, group);
		
		DEBUG_EXIT;
	} else {
		ret = __real_chown(path, owner, group);
	}
	
	free(cpath);
	
	return ret;
}

int FUNCTION_DECLARE(lchown)(const char *path, uid_t owner, gid_t group) {
	MAP(lchown, int (*)(const char *, uid_t, gid_t));
	
	int ret = 0;
	
	char *cpath = resolvePath(path);
	
	if (is_plfs_path(cpath)) {
		DEBUG_ENTER;
		
		ret = plfs_chown(cpath, owner, group);
		
		DEBUG_EXIT;
	} else {
		ret = __real_lchown(path, owner, group);
	}
	
	free(cpath);

	return ret;
}

int FUNCTION_DECLARE(fchown)(int fd, uid_t owner, gid_t group) {
	MAP(fchown, int (*)(int, uid_t, gid_t));
	
	int ret = 0;
	
	if (plfs_files.find(fd) != plfs_files.end()) {
		DEBUG_ENTER;
		
		ret = plfs_chown(plfs_files.find(fd)->second->path->c_str(), owner, group);
		
		DEBUG_EXIT;
	} else {
		ret = __real_fchown(fd, owner, group);
	}
	
	return ret;
}

/*
 * Functions to deal with renaming and file links
 *
 * readlink, link, symlink, unlink, rename
 *
 */

ssize_t FUNCTION_DECLARE(readlink)(const char *path, char *buf, size_t bufsiz) {
	MAP(readlink, ssize_t (*)(const char *, char *, size_t));
	
	ssize_t ret = 0;
	
	char *cpath = resolvePath(path);
	
	if (is_plfs_path(cpath)) {
		DEBUG_ENTER;
		
		ret = plfs_readlink(cpath, buf, bufsiz);
		
		DEBUG_EXIT;
	} else {
		ret = __real_readlink(path, buf, bufsiz);
	}
	
	free(cpath);

	return ret;
}

int FUNCTION_DECLARE(link)(const char *oldpath, const char *newpath) {
	MAP(link, int (*)(const char *, const char *));
	
	int ret = 0;
	
	char *cpath_old = resolvePath(oldpath);
	char *cpath_new = resolvePath(newpath);
	
	if (is_plfs_path(cpath_old) || is_plfs_path(cpath_new)) {
		DEBUG_ENTER;
		
		ret = -1;
		errno = ENOSYS;
		
		DEBUG_EXIT;
	} else {
		ret = __real_link(oldpath, newpath);
	}
	
	free(cpath_old);
	free(cpath_new);
	
	return ret;
}

int FUNCTION_DECLARE(symlink)(const char *oldpath, const char *newpath) {
	MAP(symlink, int (*)(const char *, const char *));
	
	int ret = 0;
	
	char *cpath_old = resolvePath(oldpath);
	char *cpath_new = resolvePath(newpath);
	
	if (is_plfs_path(cpath_old) || is_plfs_path(cpath_new)) {
		DEBUG_ENTER;
		
		ret = plfs_symlink(cpath_old, cpath_new);
		
		DEBUG_EXIT;
	} else {
		ret = __real_link(oldpath, newpath);
	}
	
	free(cpath_old);
	free(cpath_new);
	
	return ret;
}

int FUNCTION_DECLARE(unlink)(const char *pathname) {
	MAP(unlink, int (*)(const char *));
	
	int ret = 0;
	
	char *cpath = resolvePath(pathname);
	
	if (is_plfs_path(cpath)) {
		DEBUG_ENTER;
		
		ret = plfs_unlink(cpath);
		
		DEBUG_EXIT;
	} else {
		ret = __real_unlink(pathname);
	}
	
	free(cpath);

	return ret;
}

int FUNCTION_DECLARE(rename)(const char *oldpath, const char *newpath) {
	MAP(rename, int (*)(const char *, const char *));
	
	int ret = 0;
	
	char *cpath_old = resolvePath(oldpath);
	char *cpath_new = resolvePath(newpath);
	
	if (is_plfs_path(cpath_old) && is_plfs_path(cpath_new)) {
		DEBUG_ENTER;
		
		ret = plfs_rename(cpath_old, cpath_new);
		
		DEBUG_EXIT;
	} else if (is_plfs_path(cpath_old) || is_plfs_path(cpath_new)) {
		DEBUG_ENTER;
		
		ret = -1;
		errno = ENOENT;
		
		DEBUG_EXIT;
	} else {
		ret = __real_rename(oldpath, newpath);
	}

	free(cpath_old);
	free(cpath_new);

	return ret;
}
//#endif

/*
 * Functions to deal with file statistics and file system statistics
 *
 * statvfs, fstatvfs, __xstat, __fxstat, __lxstat, __xstat64, __fxstat64, __lxstat64, lgetxattr, getxattr, fgetxattr
 *
 */

int FUNCTION_DECLARE(statvfs)(const char *path, struct statvfs *buf){ 
	MAP(statvfs, int (*)(const char *, struct statvfs *));
	
	int ret;
	
	char *cpath = resolvePath(path);
	
	if (is_plfs_path(cpath)) {
		DEBUG_ENTER;
		
		ret = plfs_statvfs(cpath, buf);
		
		DEBUG_EXIT;
	} else {
		ret = __real_statvfs(path, buf);
	}
	
	free(cpath);

	return ret;
}

int FUNCTION_DECLARE(fstatvfs)(int fd, struct statvfs *buf){ 
	MAP(fstatvfs, int (*)(int, struct statvfs *));
	
	int ret;
	
	if (plfs_files.find(fd) != plfs_files.end()) {
		DEBUG_ENTER;
		
		ret = plfs_statvfs(plfs_files.find(fd)->second->path->c_str(), buf);
		
		DEBUG_EXIT;
	} else {
		ret = __real_fstatvfs(fd, buf);
	}

	return ret;
}


#ifdef XSTAT
int FUNCTION_DECLARE(__xstat)(int vers, const char *path, struct stat *buf) {
	MAP(__xstat,int (*)(int, const char*, struct stat*));

	int ret;
	
	char *cpath = resolvePath(path);
	
	if (is_plfs_path(cpath)) {
		DEBUG_ENTER;
		
		ret = plfs_getattr(NULL, cpath, buf, 0);
		
		DEBUG_EXIT;
	} else {
		ret = __real___xstat(vers, path, buf);
	}
	
	free(cpath);
	
	return ret;
}

int FUNCTION_DECLARE(__fxstat)(int vers, int fd, struct stat *buf) {
	MAP(__fxstat,int (*)(int, int, struct stat*));

	int ret;
	
	bool isdir = false;

	for (std::map<DIR *, plfs_dir *>::iterator itr = opendirs.begin(); itr != opendirs.end(); itr++) {
		if (itr->second->dirfd == fd) {
			DEBUG_ENTER;
			
			isdir = true;
			ret = plfs_getattr(NULL, itr->second->path->c_str(), buf, 0);
			
			DEBUG_EXIT;
		}
	}
	
	if (!isdir) {
		if (plfs_files.find(fd) != plfs_files.end()) {
			DEBUG_ENTER;
			
			ret = plfs_getattr(plfs_files.find(fd)->second->fd, plfs_files.find(fd)->second->path->c_str(), buf, 0);
			
			DEBUG_EXIT;
		} else {
			ret = __real___fxstat(vers, fd, buf);
		}
	}
	
	return ret;
}
#endif

int FUNCTION_DECLARE(__lxstat)(int vers, const char *path, struct stat *buf) {
	MAP(__lxstat,int (*)(int, const char*, struct stat*));
	
	int ret;
	
	char *cpath = resolvePath(path);
	
	if (is_plfs_path(cpath)) {
		DEBUG_ENTER;
		
		ret = plfs_getattr(NULL, cpath, buf, 0);
		
		DEBUG_EXIT;
	} else {
		ret = __real___lxstat(vers, path, buf);
	}
	
	free(cpath);

	return ret;
}

int FUNCTION_DECLARE(__fxstat64)(int vers, int fd, struct stat64 *buf) {
	MAP(__fxstat64,int (*)(int, int, struct stat64*));

	int ret;
	
	bool isdir = false;

	for (std::map<DIR *, plfs_dir *>::iterator itr = opendirs.begin(); itr != opendirs.end(); itr++) {
		if (itr->second->dirfd == fd) {
			DEBUG_ENTER;
			
			isdir = true;
			ret = plfs_getattr(NULL, itr->second->path->c_str(), (struct stat *) buf, 0);
			
			DEBUG_EXIT;
		}
	}
	
	if (!isdir) {
		if (plfs_files.find(fd) != plfs_files.end()) {
			DEBUG_ENTER;
			
			ret = plfs_getattr(plfs_files.find(fd)->second->fd, plfs_files.find(fd)->second->path->c_str(), (struct stat *) buf, 0);

			DEBUG_EXIT;
		} else {
			ret = __real___fxstat64(vers, fd, buf);
		}
	}

	return ret;
}

int FUNCTION_DECLARE(__lxstat64)(int vers, const char *path, struct stat64 *buf) {
	MAP(__lxstat64,int (*)(int, const char*, struct stat64*));

	int ret;
	
	char *cpath = resolvePath(path);
	
	if (is_plfs_path(cpath)) {
		DEBUG_ENTER;
		
		ret = plfs_getattr(NULL, cpath, (struct stat *) buf, 0);
		
		DEBUG_EXIT;
	} else {
		ret = __real___lxstat64(vers, path, buf);
	}
	
	free(cpath);

	return ret;
}

int FUNCTION_DECLARE(__xstat64)(int vers, const char *path, struct stat64 *buf) {
	MAP(__xstat64,int (*)(int, const char*, struct stat64*));

	int ret;
	
	char *cpath = resolvePath(path);
	
	if (is_plfs_path(cpath)) {
		DEBUG_ENTER;
		
		ret = plfs_getattr(NULL, cpath, (struct stat *) buf, 0);
		
		DEBUG_EXIT;
	} else {
		ret = __real___xstat64(vers, path, buf);
	}
	
	free(cpath);

	return ret;
}

ssize_t FUNCTION_DECLARE(lgetxattr)(const char *path, const char *name, void *value, size_t size) {
	MAP(lgetxattr, ssize_t (*)(const char *, const char *, void *, size_t));

	ssize_t ret = 0;
	
	char *cpath = resolvePath(path);
	
	if (!is_plfs_path(cpath)) ret = __real_lgetxattr(path,name,value,size);
	else {
		DEBUG_ENTER;
		DEBUG_EXIT;
	}
	
	free(cpath);
	
	return ret; 
}

size_t FUNCTION_DECLARE(getxattr)(const char *path, const char *name, void *value, size_t size) {
	MAP(getxattr, ssize_t (*)(const char *, const char *, void *, size_t));

	ssize_t ret = 0;
	
	char *cpath = resolvePath(path);
	
	if (!is_plfs_path(cpath)) ret = __real_getxattr(path,name,value,size);
	else {
		DEBUG_ENTER;
		DEBUG_EXIT;
	}
	
	free(cpath);

	return ret;
}

ssize_t FUNCTION_DECLARE(fgetxattr)(int fd, const char *name, void *value, size_t size) {
	MAP(fgetxattr, ssize_t (*)(int, const char *, void *, size_t));
	
	ssize_t ret = 0;
	
	if (plfs_files.find(fd) == plfs_files.end()) ret = __real_fgetxattr(fd, name, value, size);
	else {
		DEBUG_ENTER;
		DEBUG_EXIT;
	}

	return ret;
}

/*
 * File manipulation functions
 *
 * open, open64, creat, creat64, read, write, close, pread, pread64, pwrite, pwrite64, sync, fsync, fdatasync, syncfs, truncate, truncate64
 *
 */

int FUNCTION_DECLARE(open)(const char *path, int flags, ...) {
	MAP(open,int (*)(const char*, int, ...));
	MAP(tmpfile, FILE *(*)(void));
	
	int ret;

	char *cpath = resolvePath(path);

	if (is_plfs_path(cpath)) {
		DEBUG_ENTER;
		
		//Plfs_open_opt opts;
		//opts.index_stream = NULL;
		//opts.pinter = PLFS_MPIIO;

		mode_t mode;
		
		if ((flags & O_CREAT) == O_CREAT) {
			va_list argf;
			va_start(argf, flags);
			mode = va_arg(argf, mode_t);
			va_end(argf);
		} else {
			int m = plfs_mode(cpath, &mode);
		}
		
		plfs_file *tmp = new plfs_file();
		
		if ((errno = plfs_open(&(tmp->fd), cpath, flags, getpid(), mode, NULL)) != 0) {
			errno = -errno; // flip it correctly.
			ret = -1;
			delete tmp;
		} else {
			//ret = __real_open("/dev/random", flags & (~O_TRUNC || ~O_EXCL));
			ret = fileno(__real_tmpfile());
		
			tmp->path = new std::string(cpath);
			tmp->mode = flags;

			plfs_files.insert(std::pair<int, plfs_file *>(ret, tmp));
		}

		DEBUG_EXIT;
	} else {
		if ((flags & O_CREAT) == O_CREAT) {
			va_list argf;
			va_start(argf, flags);
			mode_t mode = va_arg(argf, mode_t);
			va_end(argf);
			ret = __real_open(path, flags, mode);
		} else {
			ret = __real_open(path, flags);
		}
	}
	
	free(cpath);
	
	return ret;
}

int FUNCTION_DECLARE(creat)(const char* path, mode_t mode) {
	int ret = FUNCTION_DECLARE(open)(path, O_WRONLY | O_CREAT | O_TRUNC, mode);
	return ret;
}

ssize_t FUNCTION_DECLARE(write)(int fd, const void *buf, size_t count) {
	MAP(write,ssize_t (*)(int, const void*, size_t));
	
	ssize_t ret;
	if (plfs_files.find(fd) != plfs_files.end()) {
		DEBUG_ENTER;
		
		off_t offset = lseek(fd, 0, SEEK_CUR);
		
		ret = plfs_write(plfs_files.find(fd)->second->fd, (const char *) buf, count, offset, getpid());

		lseek(fd, ret, SEEK_CUR);
		
		if (writesync) plfs_sync(plfs_files.find(fd)->second->fd, getpid());
		
		DEBUG_EXIT;
	} else {
		ret = __real_write(fd, buf, count);
	}

	return ret;
}

ssize_t FUNCTION_DECLARE(read)(int fd, void *buf, size_t count) {
	MAP(read,ssize_t (*)(int, void*, size_t));

	ssize_t ret;
	if (plfs_files.find(fd) != plfs_files.end()) {
		DEBUG_ENTER;
		
		off_t offset = lseek(fd, 0, SEEK_CUR);

		ret = plfs_read(plfs_files.find(fd)->second->fd, (char *) buf, count, offset);

		lseek(fd, ret, SEEK_CUR);

		if (readsync) plfs_sync(plfs_files.find(fd)->second->fd, getpid());
		
		DEBUG_EXIT;
	} else {
		ret = __real_read(fd, buf, count);
	}

	return ret;
}

int FUNCTION_DECLARE(close)(int fd) {
	
	
	MAP(close,int (*)(int));
	
	if (plfs_files.find(fd) != plfs_files.end()) {
		DEBUG_ENTER;
		
		if (!isDuplicated(fd)) {
			if (flatten_on_close) plfs_flatten_index(plfs_files.find(fd)->second->fd, plfs_files.find(fd)->second->path->c_str());
			plfs_close(plfs_files.find(fd)->second->fd, getpid(), getuid(), plfs_files.find(fd)->second->mode, NULL);
			delete plfs_files.find(fd)->second->path;
			delete plfs_files.find(fd)->second;
		}
		plfs_files.erase(fd);
		
		DEBUG_EXIT;
	}
	
	int ret = __real_close(fd);

	return ret;
}

ssize_t FUNCTION_DECLARE(pread)(int fd, void *buf, size_t count, off_t offset) {
	MAP(pread,ssize_t (*)(int, void*, size_t, off_t));
	
	ssize_t ret;
	if (plfs_files.find(fd) != plfs_files.end()) {
		DEBUG_ENTER;
		
		ret = plfs_read(plfs_files.find(fd)->second->fd, (char *) buf, count, offset);

		if (readsync) plfs_sync(plfs_files.find(fd)->second->fd, getpid());
		
		DEBUG_EXIT
	} else {
		ret = __real_pread(fd, buf, count, offset);
	}
	
	return ret;
}

ssize_t FUNCTION_DECLARE(pwrite)(int fd, const void *buf, size_t count, off_t offset) {
	MAP(pwrite,ssize_t (*)(int, const void*, size_t, off_t));

	ssize_t ret;
	if (plfs_files.find(fd) != plfs_files.end()) {
		DEBUG_ENTER;
		
		ret = plfs_write(plfs_files.find(fd)->second->fd, (const char *) buf, count, offset, getpid());

		if (writesync) plfs_sync(plfs_files.find(fd)->second->fd, getpid());
		
		DEBUG_EXIT;
	} else {
		ret = __real_pwrite(fd, buf, count, offset);
	}

	return ret;
}

int FUNCTION_DECLARE(truncate)(const char *path, off_t length) {
	MAP(truncate, int (*)(const char *, off_t));
	
	int ret;
	
	char *cpath = resolvePath(path);
	
	if (is_plfs_path(cpath)) {
		DEBUG_ENTER;
		
		ret = plfs_trunc(NULL, cpath, length); //, 0);
		
		DEBUG_EXIT;
	} else {
		ret = __real_truncate(path, length);
	}
	
	free(cpath);
	
	return ret;
}

int FUNCTION_DECLARE(truncate64)(const char *path, off64_t length) {
	MAP(truncate64, int (*)(const char *, off64_t));
	
	int ret;
	
	char *cpath = resolvePath(path);
	
	if (is_plfs_path(cpath)) {
		DEBUG_ENTER;
		
		ret = plfs_trunc(NULL, cpath, length); //, 0);
		
		DEBUG_EXIT;
	} else {
		ret = __real_truncate64(path, length);
	}
	
	free(cpath);

	return ret;
}

int FUNCTION_DECLARE(ftruncate)(int fd, off_t length) {
	MAP(ftruncate, int (*)(int, off_t));
	
	int ret;
	
	if (plfs_files.find(fd) != plfs_files.end()) {
		DEBUG_ENTER;
		
		ret = plfs_trunc(plfs_files.find(fd)->second->fd, plfs_files.find(fd)->second->path->c_str(), length); //, 1);
		
		DEBUG_EXIT;
	} else {
		ret = __real_ftruncate(fd, length);
	}

	return ret;
}

int FUNCTION_DECLARE(ftruncate64)(int fd, off64_t length) {
	MAP(ftruncate64, int (*)(int, off64_t));
	
	int ret;
	
	if (plfs_files.find(fd) != plfs_files.end()) {
		DEBUG_ENTER;
		
		ret = plfs_trunc(plfs_files.find(fd)->second->fd, plfs_files.find(fd)->second->path->c_str(), length); //, 1);
		
		DEBUG_EXIT;
	} else {
		ret = __real_ftruncate64(fd, length);
	}
	
	
	return ret;
}

void FUNCTION_DECLARE(sync)(void) {
	MAP(sync, void (*)(void));
	
	for (std::map<int, plfs_file *>::iterator itr = plfs_files.begin(); itr != plfs_files.end(); itr++) {
		DEBUG_ENTER;
		
		plfs_sync(itr->second->fd, getpid());
		
		DEBUG_EXIT;
	}
	
	__real_sync();
	
	return;
}

void FUNCTION_DECLARE(syncfs)(int fd) {
	MAP(syncfs, void (*)(int));
	
	if (plfs_files.find(fd) != plfs_files.end()) {
		DEBUG_ENTER;
		
		plfs_sync(plfs_files.find(fd)->second->fd, getpid());
		
		DEBUG_EXIT;
	} else {
		__real_syncfs(fd);
	}
	
	return;
}

int FUNCTION_DECLARE(fsync)(int fd) {
	MAP(fsync, int (*)(int));
	
	int ret = 0;
	
	if (plfs_files.find(fd) != plfs_files.end()) {
		DEBUG_ENTER;

		ret = plfs_sync(plfs_files.find(fd)->second->fd, getpid());

		DEBUG_EXIT;
	} else {
		ret = __real_fsync(fd);
	}
	
	return ret;
}

int FUNCTION_DECLARE(fdatasync)(int fd) {
	MAP(fdatasync, int (*)(int));
	
	int ret = 0;
	
	if (plfs_files.find(fd) != plfs_files.end()) {
		DEBUG_ENTER;
		
		ret = plfs_sync(plfs_files.find(fd)->second->fd, getpid());
		
		DEBUG_EXIT;
	} else {
		ret = __real_fdatasync(fd);
	}

	return ret;
}

int FUNCTION_DECLARE(open64)(const char *path, int flags, ...) {
	MAP(open64,int (*)(const char*, int, ...));
	MAP(tmpfile, FILE *(*)(void));
	
	int ret;
	
	char *cpath = resolvePath(path);

	if (is_plfs_path(cpath)) {
		DEBUG_ENTER;
		
		//Plfs_open_opt opts;
		//opts.index_stream = NULL;
		//opts.pinter = PLFS_MPIIO;

		mode_t mode;
		
		if ((flags & O_CREAT) == O_CREAT) {
			va_list argf;
			va_start(argf, flags);
			mode = va_arg(argf, mode_t);
			va_end(argf);
		} else {
			plfs_mode(cpath, &mode);
		}
		
		plfs_file *tmp = new plfs_file();
		

		if ((errno = plfs_open(&(tmp->fd), cpath, flags, getpid(), mode, NULL)) != 0) {
			errno = -errno; // flip it correctly
			ret = -1;
			delete tmp;
		} else {
			//ret = __real_open("/dev/random", flags & (~O_TRUNC || ~O_EXCL));
			ret = fileno(__real_tmpfile());

			tmp->path = new std::string(cpath);
			tmp->mode = flags;

			plfs_files.insert(std::pair<int, plfs_file *>(ret, tmp));
		}
		
		DEBUG_EXIT;
	} else {
		if ((flags & O_CREAT) == O_CREAT) {
			va_list argf;
			va_start(argf, flags);
			mode_t mode = va_arg(argf, mode_t);
			va_end(argf);
			ret = __real_open64(path, flags, mode);
		} else {
			ret = __real_open64(path, flags);
		}
	}
	
	free(cpath);
	
	return ret;
}

int FUNCTION_DECLARE(creat64)(const char* path, mode_t mode) {
	int ret = FUNCTION_DECLARE(open64)(path, O_WRONLY | O_CREAT | O_TRUNC, mode);
	return ret;
}

ssize_t FUNCTION_DECLARE(pread64)(int fd, void *buf, size_t count, off64_t offset) {
	MAP(pread64,ssize_t (*)(int, void*, size_t, off64_t));

	ssize_t ret;
	if (plfs_files.find(fd) != plfs_files.end()) {
		DEBUG_ENTER;
		
		ret = plfs_read(plfs_files.find(fd)->second->fd, (char *) buf, count, offset);

		if (readsync) plfs_sync(plfs_files.find(fd)->second->fd, getpid());
		
		DEBUG_EXIT;
	} else {
		ret = __real_pread64(fd, buf, count, offset);
	}
	
	return ret;
}

ssize_t FUNCTION_DECLARE(pwrite64)(int fd, const void *buf, size_t count, off64_t offset) {
	MAP(pwrite64,ssize_t (*)(int, const void*, size_t, off64_t));

	ssize_t ret;
	if (plfs_files.find(fd) != plfs_files.end()) {
		DEBUG_ENTER;
		
		ret = plfs_write(plfs_files.find(fd)->second->fd, (const char *) buf, count, offset, getpid());

		if (writesync) plfs_sync(plfs_files.find(fd)->second->fd, getpid());
		
		DEBUG_EXIT;
	} else {
		ret = __real_pwrite64(fd, buf, count, offset);
	}

	return ret;
}

#ifdef __cplusplus
}
#endif

#pragma GCC visibility push(default)

