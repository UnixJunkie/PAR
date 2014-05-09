#!/usr/bin/env bash

#set -x

# enforce parameters
if [ "$#" != "3" ] ; then
    echo "usage: "$0" par_command_path server_name machinefile"
    echo "  machinefile line format is 'hostname[:max_nprocs]'"
    echo "  if a line doesn't have ':', all CPUs of this machine will be used"
    exit 1
fi

par_exe=$1
server=$2
machinefile=$3

# start workers on machines without an nprocs limit in the machinefile
for m in `grep -v ':' $machinefile` ; do
    ssh $m "nohup $par_exe -c $server < /dev/null 2>&1 > /dev/null &"
done
# start remaining workers
for line in `grep ':' $machinefile` ; do
    m=`echo $line | cut -d':' -f1`
    nprocs=`echo $line | cut -d':' -f2`
    ssh $m "nohup $par_exe -c $server -w $nprocs < /dev/null 2>&1 > /dev/null &"
done
