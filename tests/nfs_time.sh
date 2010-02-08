#!/bin/bash

# time fresh file checkout from NFS to /tmp on several hosts

set -x

for i in `echo ~/1 ~/2 ~/4 ~/8 ~/16 ~/32 ~/64 ~/128 ~/256 ~/512 ~/1024` ; do
    for h in `echo w l p k` ; do # edit your hosts list here
        ssh $h time cp $i /tmp/test
    done
done
