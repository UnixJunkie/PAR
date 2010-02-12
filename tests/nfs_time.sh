#!/bin/bash

# time fresh file checkout from NFS to /tmp on several hosts

# set -x

# for i in `echo ~/1 ~/2 ~/4 ~/8 ~/16 ~/32 ~/64 ~/128 ~/256 ~/512 ~/1024` ; do
#     for h in `echo w l p k` ; do # edit your hosts list here
#         ssh $h time cp $i /tmp/test
#     done
# done

time cp ~/1_n/1 /tmp    | grep real >> /tmp/1_n.txt
time cp ~/1_n/2 /tmp    | grep real >> /tmp/1_n.txt
time cp ~/1_n/4 /tmp    | grep real >> /tmp/1_n.txt
time cp ~/1_n/8 /tmp    | grep real >> /tmp/1_n.txt
time cp ~/1_n/16 /tmp   | grep real >> /tmp/1_n.txt
time cp ~/1_n/32 /tmp   | grep real >> /tmp/1_n.txt
time cp ~/1_n/64 /tmp   | grep real >> /tmp/1_n.txt
time cp ~/1_n/128 /tmp  | grep real >> /tmp/1_n.txt
time cp ~/1_n/256 /tmp  | grep real >> /tmp/1_n.txt
time cp ~/1_n/512 /tmp  | grep real >> /tmp/1_n.txt
time cp ~/1_n/1024 /tmp | grep real >> /tmp/1_n.txt

time cp ~/2_n/1 /tmp    | grep real >> /tmp/2_n.txt
time cp ~/2_n/2 /tmp    | grep real >> /tmp/2_n.txt
time cp ~/2_n/4 /tmp    | grep real >> /tmp/2_n.txt
time cp ~/2_n/8 /tmp    | grep real >> /tmp/2_n.txt
time cp ~/2_n/16 /tmp   | grep real >> /tmp/2_n.txt
time cp ~/2_n/32 /tmp   | grep real >> /tmp/2_n.txt
time cp ~/2_n/64 /tmp   | grep real >> /tmp/2_n.txt
time cp ~/2_n/128 /tmp  | grep real >> /tmp/2_n.txt
time cp ~/2_n/256 /tmp  | grep real >> /tmp/2_n.txt
time cp ~/2_n/512 /tmp  | grep real >> /tmp/2_n.txt
time cp ~/2_n/1024 /tmp | grep real >> /tmp/2_n.txt

time cp ~/4_n/1 /tmp    | grep real >> /tmp/4_n.txt
time cp ~/4_n/2 /tmp    | grep real >> /tmp/4_n.txt
time cp ~/4_n/4 /tmp    | grep real >> /tmp/4_n.txt
time cp ~/4_n/8 /tmp    | grep real >> /tmp/4_n.txt
time cp ~/4_n/16 /tmp   | grep real >> /tmp/4_n.txt
time cp ~/4_n/32 /tmp   | grep real >> /tmp/4_n.txt
time cp ~/4_n/64 /tmp   | grep real >> /tmp/4_n.txt
time cp ~/4_n/128 /tmp  | grep real >> /tmp/4_n.txt
time cp ~/4_n/256 /tmp  | grep real >> /tmp/4_n.txt
time cp ~/4_n/512 /tmp  | grep real >> /tmp/4_n.txt
time cp ~/4_n/1024 /tmp | grep real >> /tmp/4_n.txt

time cp ~/8_n/1 /tmp    | grep real >> /tmp/8_n.txt
time cp ~/8_n/2 /tmp    | grep real >> /tmp/8_n.txt
time cp ~/8_n/4 /tmp    | grep real >> /tmp/8_n.txt
time cp ~/8_n/8 /tmp    | grep real >> /tmp/8_n.txt
time cp ~/8_n/16 /tmp   | grep real >> /tmp/8_n.txt
time cp ~/8_n/32 /tmp   | grep real >> /tmp/8_n.txt
time cp ~/8_n/64 /tmp   | grep real >> /tmp/8_n.txt
time cp ~/8_n/128 /tmp  | grep real >> /tmp/8_n.txt
time cp ~/8_n/256 /tmp  | grep real >> /tmp/8_n.txt
time cp ~/8_n/512 /tmp  | grep real >> /tmp/8_n.txt
time cp ~/8_n/1024 /tmp | grep real >> /tmp/8_n.txt
