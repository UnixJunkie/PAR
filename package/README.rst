# Some usage examples:

# run in parallel commands from test_parallel.input
parallel.py -i test_parallel.input

# same than before but with a progress bar and reading from stdin
cat test_parallel.input | parallel.py -i /dev/stdin -v

# run in parallel commands from test_parallel.input and store output
# in output.log
parallel.py -i test_parallel.input -o output.log

## real world usage example
# 1) server side
parallel.py -v -i many_commands.sh -o par_many_commands.log -s
# 2) client side, on each machine you want to join the computation
#    replace SERVER_NAME by the machine name from where you launched
#    parallel.py using -s
parallel.py -c SERVER_NAME
# 3) be thrilled! ;)

If you use this software for publication, please cite the corresponding
publication:

@article{Berenger2010,
author = {Berenger, Francois and Coti, Camille and Zhang, Kam Y. J.},
title = {{PAR: A PARallel And Distributed Job Crusher}},
year = {2010},
journal = {Bioinformatics},
doi = {10.1093/bioinformatics/btq542},
url = {http://bioinformatics.oxfordjournals.org/content/26/22/2918.full}
}
