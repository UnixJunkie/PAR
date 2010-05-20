#!/bin/bash

#set -x # debug

PAR_ROOT=~/src/par              # where you downloaded par
export PYTHONPATH=$PAR_ROOT/lib # where par's Pyro is
export PATH=/usr/local/src/Python-2.4.6/bin:$PATH # to find python2.4 if it is
                                                  # not the default one
$PAR_ROOT/src/parallel.py "$@"  # call it
