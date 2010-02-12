#!/bin/bash

# time fresh file checkout from NFS to /tmp on several hosts

# set -x

# for i in `echo ~/1 ~/2 ~/4 ~/8 ~/16 ~/32 ~/64 ~/128 ~/256 ~/512 ~/1024` ; do
#     for h in `echo w l p k` ; do # edit your hosts list here
#         ssh $h time cp $i /tmp/test
#     done
# done

# node1, node2, 
(time cp ~/1_n_/1 /tmp    ) 2>&1 | grep real > /tmp/1_n.txt
(time cp ~/1_n_/2 /tmp    ) 2>&1 | grep real >> /tmp/1_n.txt
(time cp ~/1_n_/4 /tmp    ) 2>&1 | grep real >> /tmp/1_n.txt
(time cp ~/1_n_/8 /tmp    ) 2>&1 | grep real >> /tmp/1_n.txt
(time cp ~/1_n_/16 /tmp   ) 2>&1 | grep real >> /tmp/1_n.txt
(time cp ~/1_n_/32 /tmp   ) 2>&1 | grep real >> /tmp/1_n.txt
(time cp ~/1_n_/64 /tmp   ) 2>&1 | grep real >> /tmp/1_n.txt
(time cp ~/1_n_/128 /tmp  ) 2>&1 | grep real >> /tmp/1_n.txt
(time cp ~/1_n_/256 /tmp  ) 2>&1 | grep real >> /tmp/1_n.txt
(time cp ~/1_n_/512 /tmp  ) 2>&1 | grep real >> /tmp/1_n.txt
(time cp ~/1_n_/1024 /tmp ) 2>&1 | grep real >> /tmp/1_n.txt

# node 1*2 3*4
(time cp ~/2_n_/1 /tmp    ) 2>&1 | grep real > /tmp/2_n.txt
(time cp ~/2_n_/2 /tmp    ) 2>&1 | grep real >> /tmp/2_n.txt
(time cp ~/2_n_/4 /tmp    ) 2>&1 | grep real >> /tmp/2_n.txt
(time cp ~/2_n_/8 /tmp    ) 2>&1 | grep real >> /tmp/2_n.txt
(time cp ~/2_n_/16 /tmp   ) 2>&1 | grep real >> /tmp/2_n.txt
(time cp ~/2_n_/32 /tmp   ) 2>&1 | grep real >> /tmp/2_n.txt
(time cp ~/2_n_/64 /tmp   ) 2>&1 | grep real >> /tmp/2_n.txt
(time cp ~/2_n_/128 /tmp  ) 2>&1 | grep real >> /tmp/2_n.txt
(time cp ~/2_n_/256 /tmp  ) 2>&1 | grep real >> /tmp/2_n.txt
(time cp ~/2_n_/512 /tmp  ) 2>&1 | grep real >> /tmp/2_n.txt
(time cp ~/2_n_/1024 /tmp ) 2>&1 | grep real >> /tmp/2_n.txt

# node 1*2*3*4 5*6*7*8
(time cp ~/4_n_/1 /tmp    ) 2>&1 | grep real > /tmp/4_n.txt
(time cp ~/4_n_/2 /tmp    ) 2>&1 | grep real >> /tmp/4_n.txt
(time cp ~/4_n_/4 /tmp    ) 2>&1 | grep real >> /tmp/4_n.txt
(time cp ~/4_n_/8 /tmp    ) 2>&1 | grep real >> /tmp/4_n.txt
(time cp ~/4_n_/16 /tmp   ) 2>&1 | grep real >> /tmp/4_n.txt
(time cp ~/4_n_/32 /tmp   ) 2>&1 | grep real >> /tmp/4_n.txt
(time cp ~/4_n_/64 /tmp   ) 2>&1 | grep real >> /tmp/4_n.txt
(time cp ~/4_n_/128 /tmp  ) 2>&1 | grep real >> /tmp/4_n.txt
(time cp ~/4_n_/256 /tmp  ) 2>&1 | grep real >> /tmp/4_n.txt
(time cp ~/4_n_/512 /tmp  ) 2>&1 | grep real >> /tmp/4_n.txt
(time cp ~/4_n_/1024 /tmp ) 2>&1 | grep real >> /tmp/4_n.txt

# nodes 1..8
(time cp ~/8_n_/1 /tmp    ) 2>&1 | grep real > /tmp/8_n.txt
(time cp ~/8_n_/2 /tmp    ) 2>&1 | grep real >> /tmp/8_n.txt
(time cp ~/8_n_/4 /tmp    ) 2>&1 | grep real >> /tmp/8_n.txt
(time cp ~/8_n_/8 /tmp    ) 2>&1 | grep real >> /tmp/8_n.txt
(time cp ~/8_n_/16 /tmp   ) 2>&1 | grep real >> /tmp/8_n.txt
(time cp ~/8_n_/32 /tmp   ) 2>&1 | grep real >> /tmp/8_n.txt
(time cp ~/8_n_/64 /tmp   ) 2>&1 | grep real >> /tmp/8_n.txt
(time cp ~/8_n_/128 /tmp  ) 2>&1 | grep real >> /tmp/8_n.txt
(time cp ~/8_n_/256 /tmp  ) 2>&1 | grep real >> /tmp/8_n.txt
(time cp ~/8_n_/512 /tmp  ) 2>&1 | grep real >> /tmp/8_n.txt
(time cp ~/8_n_/1024 /tmp ) 2>&1 | grep real >> /tmp/8_n.txt

# src/dm.py
# server-side puts
export PATH=/usr/local/src/Python-2.4.6/bin:$PATH
export PYTHONPATH=~/usr/Pyro-3.10/build/lib
~/src/par/src/dm.py
~/src/par/src/dm.py put ~/8_n_/1    ~/8_n_/1
~/src/par/src/dm.py put ~/8_n_/2    ~/8_n_/2
~/src/par/src/dm.py put ~/8_n_/4    ~/8_n_/4
~/src/par/src/dm.py put ~/8_n_/8    ~/8_n_/8
~/src/par/src/dm.py put ~/8_n_/16   ~/8_n_/16
~/src/par/src/dm.py put ~/8_n_/32   ~/8_n_/32
~/src/par/src/dm.py put ~/8_n_/64   ~/8_n_/64
~/src/par/src/dm.py put ~/8_n_/128  ~/8_n_/128
~/src/par/src/dm.py put ~/8_n_/256  ~/8_n_/256
~/src/par/src/dm.py put ~/8_n_/512  ~/8_n_/512
~/src/par/src/dm.py put ~/8_n_/1024 ~/8_n_/1024

# node1, node2
export PATH=/usr/local/src/Python-2.4.6/bin:$PATH
export PYTHONPATH=~/usr/Pyro-3.10/build/lib
~/src/par/src/dm.py rmdm bragg, ls
(time ~/src/par/src/dm.py get ~/8_n_/1    /tmp/1      ) 2>&1 | grep real >  /tmp/1_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/2    /tmp/2      ) 2>&1 | grep real >> /tmp/1_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/4    /tmp/4      ) 2>&1 | grep real >> /tmp/1_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/8    /tmp/8      ) 2>&1 | grep real >> /tmp/1_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/16   /tmp/16     ) 2>&1 | grep real >> /tmp/1_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/32   /tmp/32     ) 2>&1 | grep real >> /tmp/1_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/64   /tmp/64     ) 2>&1 | grep real >> /tmp/1_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/128  /tmp/128    ) 2>&1 | grep real >> /tmp/1_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/256  /tmp/256    ) 2>&1 | grep real >> /tmp/1_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/512  /tmp/512    ) 2>&1 | grep real >> /tmp/1_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/1024 /tmp/1024   ) 2>&1 | grep real >> /tmp/1_n_dfs.txt
~/src/par/src/dm.py k

# node3*node4
export PATH=/usr/local/src/Python-2.4.6/bin:$PATH
export PYTHONPATH=~/usr/Pyro-3.10/build/lib
~/src/par/src/dm.py rmdm bragg, ls
(time ~/src/par/src/dm.py get ~/8_n_/1    /tmp/1      ) 2>&1 | grep real >  /tmp/2_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/2    /tmp/2      ) 2>&1 | grep real >> /tmp/2_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/4    /tmp/4      ) 2>&1 | grep real >> /tmp/2_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/8    /tmp/8      ) 2>&1 | grep real >> /tmp/2_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/16   /tmp/16     ) 2>&1 | grep real >> /tmp/2_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/32   /tmp/32     ) 2>&1 | grep real >> /tmp/2_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/64   /tmp/64     ) 2>&1 | grep real >> /tmp/2_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/128  /tmp/128    ) 2>&1 | grep real >> /tmp/2_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/256  /tmp/256    ) 2>&1 | grep real >> /tmp/2_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/512  /tmp/512    ) 2>&1 | grep real >> /tmp/2_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/1024 /tmp/1024   ) 2>&1 | grep real >> /tmp/2_n_dfs.txt
~/src/par/src/dm.py k

# node 5*6*7*8
export PATH=/usr/local/src/Python-2.4.6/bin:$PATH
export PYTHONPATH=~/usr/Pyro-3.10/build/lib
~/src/par/src/dm.py rmdm bragg, ls
(time ~/src/par/src/dm.py get ~/8_n_/1    /tmp/1      ) 2>&1 | grep real >  /tmp/4_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/2    /tmp/2      ) 2>&1 | grep real >> /tmp/4_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/4    /tmp/4      ) 2>&1 | grep real >> /tmp/4_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/8    /tmp/8      ) 2>&1 | grep real >> /tmp/4_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/16   /tmp/16     ) 2>&1 | grep real >> /tmp/4_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/32   /tmp/32     ) 2>&1 | grep real >> /tmp/4_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/64   /tmp/64     ) 2>&1 | grep real >> /tmp/4_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/128  /tmp/128    ) 2>&1 | grep real >> /tmp/4_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/256  /tmp/256    ) 2>&1 | grep real >> /tmp/4_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/512  /tmp/512    ) 2>&1 | grep real >> /tmp/4_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/1024 /tmp/1024   ) 2>&1 | grep real >> /tmp/4_n_dfs.txt
~/src/par/src/dm.py k

export PATH=/usr/local/src/Python-2.4.6/bin:$PATH
export PYTHONPATH=~/usr/Pyro-3.10/build/lib
~/src/par/src/dm.py rmdm bragg, ls
(time ~/src/par/src/dm.py get ~/8_n_/1    /tmp/1      ) 2>&1 | grep real >  /tmp/8_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/2    /tmp/2      ) 2>&1 | grep real >> /tmp/8_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/4    /tmp/4      ) 2>&1 | grep real >> /tmp/8_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/8    /tmp/8      ) 2>&1 | grep real >> /tmp/8_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/16   /tmp/16     ) 2>&1 | grep real >> /tmp/8_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/32   /tmp/32     ) 2>&1 | grep real >> /tmp/8_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/64   /tmp/64     ) 2>&1 | grep real >> /tmp/8_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/128  /tmp/128    ) 2>&1 | grep real >> /tmp/8_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/256  /tmp/256    ) 2>&1 | grep real >> /tmp/8_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/512  /tmp/512    ) 2>&1 | grep real >> /tmp/8_n_dfs.txt
(time ~/src/par/src/dm.py get ~/8_n_/1024 /tmp/1024   ) 2>&1 | grep real >> /tmp/8_n_dfs.txt
~/src/par/src/dm.py k
