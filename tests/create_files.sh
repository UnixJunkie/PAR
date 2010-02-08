#!/bin/bash

# create files for perf tests with size from 1 to 1024M

set -x

INPUT=/tmp/big_file

for i in `echo 1 2 4 8 16 32 64 128 256 512 1024` ; do
    dd if=$INPUT of=$i bs=$i"M" count=1
done
