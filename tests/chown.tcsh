#! /bin/tcsh -xf

set mnt  = "/mnt/plfs"
set top  = $mnt/$USER 
set ts   = `date +%s`
set file = $top/foo.$ts

mkdir -p $mnt/$USER
if ( $? != 0 ) then
    echo "mkdir error"
    exit 1
endif

touch $file
if ( $? != 0 ) then
    echo "touch error"
    exit 1
endif

# ugh, this is finished at all.
# prolly should do it in perl
# problem is have to be smart to check the stat result
# also should query groups to be smart about who to chown things too
ls -lt $file
chown johnbent.cosmo $file
ls -lt $file
rm $file
