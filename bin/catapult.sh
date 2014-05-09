#!/usr/bin/env bash

set -x

# enforce parameters
if [ "$#" != "3" ] ; then
    echo "usage: "$0" par_exe_path server_name machinefile"
    exit 1
fi

par_exe=$1
server=$2
machinefile=$3

# start workers on machines without an nprocs limit
for m in `grep -v ':' $machinefile` ; do
    ssh $m $par_exe -c $server
done

# start workers on machines with an nproc limit
for line in `grep ':' $machinefile` ; do
    m=`echo $line | cut -d':' -f1`
    nprocs=`echo $line | cut -d':' -f2`
    ssh $m $par_exe -c $server -w $nprocs
done
