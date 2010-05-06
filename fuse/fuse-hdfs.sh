set -u
export CLASSPATH="$HADOOP_HOME/hadoop-0.20.2-core.jar:$HADOOP_HOME/lib/commons-logging-api-1.0.4.jar"
export CLASSPATH=$CLASSPATH:"$HADOOP_HOME/conf"
echo Classpath: $CLASSPATH

export PLFS_MNT=/tmp/mnt-fuse
export PLFS_BACK=/user/eestolan
#export PLFS_DEBUG=/dev/null
#make smount # background process
make lmount # foreground, but backgrounded and stdout sent to /tmp/plfs.log



###OLD STUFF

# Old deprecated way to mount
# gdb --args ./plfs "$@"
# ./plfs "$@"

# Example: ./fuse.sh -d /tmp/mnt-fuse/ -plfs_backend=/

#export LIBHDFS_OPTS="-XX:+TraceClassLoading -XX:ErrorFile=./hs_err_pid<pid>.log "\
#"-XX:LargePageSizeInBytes=4m -Xmx2000m"



# Extra classpaths:


#"/cse/grads/eestolan/hadoop-0.20.2/hadoop-0.20.2-ant.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/hadoop-0.20.2-examples.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/hadoop-0.20.2-test.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/hadoop-0.20.2-tools.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/lib/commons-cli-1.2.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/lib/commons-codec-1.3.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/lib/commons-el-1.0.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/lib/commons-httpclient-3.0.1.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/lib/commons-logging-1.0.4.jar:"\

#"/cse/grads/eestolan/hadoop-0.20.2/lib/commons-net-1.4.1.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/lib/core-3.1.1.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/lib/hsqldb-1.8.0.10.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/lib/jasper-compiler-5.5.12.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/lib/jasper-runtime-5.5.12.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/lib/jets3t-0.6.1.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/lib/jetty-6.1.14.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/lib/jetty-util-6.1.14.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/lib/junit-3.8.1.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/lib/kfs-0.2.2.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/lib/log4j-1.2.15.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/lib/oro-2.0.8.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/lib/servlet-api-2.5-6.1.14.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/lib/slf4j-api-1.4.3.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/lib/slf4j-log4j12-1.4.3.jar:"\
#"/cse/grads/eestolan/hadoop-0.20.2/lib/xmlenc-0.52.jar"


