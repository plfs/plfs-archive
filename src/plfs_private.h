#ifndef __PLFS_PRIVATE__
#define __PLFS_PRIVATE__

#include "plfs_internal.h"

#include <map>
#include <set>
#include <string>
#include <vector>
using namespace std;

#define SVNVERS $Rev$

typedef enum {
    CONTAINER,
    FLAT_FILE
} plfs_filetype;

typedef struct {
    string mnt_pt;  // the logical mount point
    string *statfs; // where to resolve statfs calls
    string *syncer_ip; // where to send commands within plfs_protect
    vector<string> backends;    // a list of physical locations 
    vector<string> canonical_backends;
    vector<string> shadow_backends;
    vector<string> mnt_tokens;
    plfs_filetype file_type;
    unsigned checksum;
} PlfsMount;

// some functions require that the path passed be a PLFS path
// some (like symlink) don't
enum
requirePlfsPath {
    PLFS_PATH_REQUIRED,
    PLFS_PATH_NOTREQUIRED,
};

enum
expansionMethod {
    EXPAND_CANONICAL,
    EXPAND_SHADOW,
    EXPAND_TO_I,
};

typedef struct {
    bool is_mnt_pt;
    bool expand_error;
    PlfsMount *mnt_pt;
    int Errno;  // can't use just errno, it's a weird macro
    string expanded;
    string backend; // I tried to not put this in to save space . . .  
} ExpansionInfo;

// a struct containing all the various containers paths for a logical file
typedef struct {
    string shadow;            // full path to shadow container
    string canonical;         // full path to canonical
    string hostdir;           // the name of the hostdir itself
    string shadow_hostdir;    // full path to shadow hostdir
    string canonical_hostdir; // full path to the canonical hostdir
    string shadow_backend;    // full path of shadow backend 
    string canonical_backend; // full path of canonical backend 
} ContainerPaths;

string
expandPath(string logical, ExpansionInfo *exp_info, 
        expansionMethod hash_method, int which_backend, int depth);

#define PLFS_ENTER PLFS_ENTER2(PLFS_PATH_REQUIRED)

#define PLFS_ENTER2(X) \
 int ret = 0;\
 ExpansionInfo expansion_info; \
 string path = expandPath(logical,&expansion_info,EXPAND_CANONICAL,-1,0); \
 plfs_debug("EXPAND in %s: %s->%s\n",__FUNCTION__,logical,path.c_str()); \
 if (expansion_info.expand_error && X==PLFS_PATH_REQUIRED) { \
     PLFS_EXIT(-ENOENT); \
 } \
 if (expansion_info.Errno && X==PLFS_PATH_REQUIRED) { \
     PLFS_EXIT(expansion_info.Errno); \
 }

#define PLFS_EXIT(X) return(X);

#define EISDIR_DEBUG \
    if(ret!=0) {\
        Util::OpenError(__FILE__,__FUNCTION__,__LINE__,pid,errno);\
    }

typedef struct {
    string file; // which top-level plfsrc was used 
    set<string> files;
    size_t num_hostdirs;
    size_t threadpool_size;
    size_t buffer_mbs;  // how many mbs to buffer for write indexing
    map<string,PlfsMount*> mnt_pts;
    bool direct_io; // a flag FUSE needs.  Sorry ADIO and API for the wasted bit
    string *err_msg;
    string *global_summary_dir;
    PlfsMount *tmp_mnt; // just used during parsing
} PlfsConf;

/* get_plfs_conf
   get a pointer to a struct holding plfs configuration values
   parse $HOME/.plfsrc or /etc/plfsrc to find parameter values
   if root, check /etc/plfsrc first and then if fail, then check $HOME/.plfsrc
   if not root, reverse order
*/
PlfsConf* get_plfs_conf( );  

PlfsMount * find_mount_point(PlfsConf *pconf, const string &path, bool &found);
PlfsMount * find_mount_point_using_tokens(PlfsConf *, vector <string> &, bool&);

/* plfs_init
    it just warms up the plfs structures used in expandPath
*/
bool plfs_init(PlfsConf*);

int plfs_chmod_cleanup(const char *logical,mode_t mode );
int plfs_chown_cleanup (const char *logical,uid_t uid,gid_t gid );

ssize_t plfs_reference_count( Plfs_fd * );
void plfs_stat_add(const char*func, double time, int );

int plfs_mutex_lock( pthread_mutex_t *mux, const char *whence );
int plfs_mutex_unlock( pthread_mutex_t *mux, const char *whence );

uid_t plfs_getuid();
gid_t plfs_getgid();
int plfs_setfsuid(uid_t);
int plfs_setfsgid(gid_t);

#endif
