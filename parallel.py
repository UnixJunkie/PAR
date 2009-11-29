#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Copyright (C) 2009, Zhang Initiative Research Unit,
Advance Science Institute, Riken
2-1 Hirosawa, Wako, Saitama 351-0198, Japan
If you use and like our software, please send us a postcard! ^^
---
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
---
Read one command from each line of commands_file, execute several in
parallel.
'xargs -P' has something almost similar, but not exactly what we need.
Your machine's number of cores is the default parallelization factor.

warning: keep this script compatible with python 2.4 so we can run it
on old systems too

TODO:
 * add hook for post processing of (cmd, res) couples by using some
   python function/code from a user specified file
 * profile this program with the python profiler
   check if semaphore would be more efficient than using python Queue
   objects
 * correct this low priority bug (program should exit cleanly instead of
   throwing an exception):
   user@host# parallel.py -i /dev/null
   exception:  exceptions.ZeroDivisionError
"""

import commands, string, sys, thread

from Queue import Queue, Empty
from ProgressBar import ProgressBar

def usage():
    print ("usage: ./parallel.py -i ...")
    print ("{-i  | --input} commands_file /dev/stdin for example")
    print ("[{-o | --output} output_file] log to file instead of stdout")
    print ("[{-v | --verbose}]            enables progress bar")
    print ("[{-w | --workers}] n          working threads (default is " +
           str(get_nb_procs()) + ")")
    print ("                              must have: n > 0")
    sys.exit(0)

# return cmd and its output as a parsable string like:
# cmd("cat myfile"):res("myfile_content")
def echo_command(cmd):
    cmd_str = cmd
    cmd_out = commands.getoutput(cmd)
    return "cmd(" + cmd_str + "):res(" + cmd_out + ")"

def get_nb_procs():
    return int(commands.getoutput("egrep -c '^processor' /proc/cpuinfo"))

def worker_wrapper(commands_queue, results_queue, lock):
    try:
        while (True):
            cmd = commands_queue.get(True, 1)
            command_and_res = echo_command(cmd)
            results_queue.put(command_and_res)
    except Empty:
        lock.release()

# return index in lst of the first element from elt_lst found in lst
# return -1 if none found
def first_index_lst(elt_lst, lst):
    # return index of elt in lst, -1 if not found
    def first_index(elt, lst):
        res = -1
        n = len(lst)
        i = 0
        while i < n and lst[i] != elt:
            i = i+1
        if i < n:
            res = i
        return res
    res = -1
    for elt in elt_lst:
        idx = first_index(elt, lst)
        if idx != -1:
            res = idx
            break
    return res

if __name__ == '__main__':
    try:
        show_progress  = False
        output_to_file = False
        commands_file  = None
        output_file    = None
        args           = sys.argv
        nb_threads     = get_nb_procs()
        output_param = first_index_lst(["-o","--output"], args)
        if output_param != -1:
            output_to_file = True
            output_file_param = args[output_param + 1]
            output_file       = open(output_file_param, 'a')
        input_param = first_index_lst(["-i","--input"], args)
        if input_param != -1:  # mandatory option
            commands_file_param = args[input_param + 1]
            commands_file  = open(commands_file_param, 'r')
        else:
            usage()
        if first_index_lst(["-v","--verbose"], args) != -1:
            show_progress = True
        nb_workers_param = first_index_lst(["-w","--workers"], args)
        if nb_workers_param != -1:
            nb_threads = int(args[nb_workers_param + 1])
            if not nb_threads > 0:
                usage()
        commands_queue = Queue()
        results_queue  = Queue()
        nb_jobs        = 0
        locks          = []
        for i in range(nb_threads):
            l = thread.allocate_lock()
            l.acquire()
            locks.append(l)
            thread.start_new_thread(worker_wrapper,
                                    (commands_queue, results_queue, l))
        for cmd in commands_file:
            commands_queue.put(string.strip(cmd))
            nb_jobs = nb_jobs + 1
        progress_bar = ProgressBar(0, nb_jobs, 60)
        # output everything
        jobs_done = 0
        if show_progress:
            progress_bar.draw()
        while jobs_done < nb_jobs:
            res = results_queue.get()
            jobs_done = jobs_done + 1
            if output_to_file:
                output_file.write(res + '\n')
            if show_progress:
                progress_bar.updateAmount(jobs_done)
                progress_bar.draw()
            elif not output_to_file:
                print res
        # wait for everybody
        for l in locks:
            l.acquire()
        # close all files
        commands_file.close()
        if output_to_file:
            output_file.close()
    except SystemExit:
        pass
    except: # unexpected one
        print "exception: ", sys.exc_info()[0]
