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

warning: keep this script compatible with python 2.4 so that we can run it
         on old systems too

TODO:
 * implement this ?
   The client could be at the same time a server for other clients in order to
   scale by using hierarchical layers of servers instead of having only one
   server managing too many clients (russian doll/fractal architecture)
 * think about the security of the client-server model:
   - a client shouldn't accept to execute commands from an untrusted server
     (commands could be any Unix command, including rm)
   - server shouldn't accept to give commands to untrusted clients
     (this would deplete the commands list probably without doing the
      corresponding jobs and sending back the results)
 * for -p|--post: return both (stdin, stdout and stderr to the user), not just
   (stdin, stdout) like actually, so that he can troubleshoot if some of his
   commands fail in error by reading their stderr
 * profile with the python profiler
 * add a code coverage test, python is not compiled and pychecker is too
   light at ckecking things
 * correct this harmless bug (program should exit cleanly instead of
   throwing an exception):
   user@host# parallel.py -i /dev/null
   exception:  exceptions.ZeroDivisionError
"""

import commands, os, string, sys, time, thread
import Pyro.core, Pyro.naming

from Queue import Queue, Empty
from ProgressBar import ProgressBar
from Pyro.errors import PyroError, NamingError, ConnectionClosedError

class Master(Pyro.core.ObjBase):
    def __init__(self, commands_q, results_q):
        Pyro.core.ObjBase.__init__(self)
        self.jobs_queue     = commands_q
        self.results_queue  = results_q

    def get_work(self):
        res = ""
        try:
            res = self.jobs_queue.get(True, 1)
        except Empty:
            pass
        return res

    def put_result(self, cmd_and_res):
        self.results_queue.put(cmd_and_res)

    def add_job(self, cmd):
        self.jobs_queue.put(cmd)

def usage():
    print ("Usage: parallel.py [options] -i | -c ...")
    print ("Execute commands in parallel.")
    print ("")
    print ("  [-h | --help]               you are currently reading it")
    print ("  -c  | --client              BETA/EXPERIMENTAL feature")
    print ("                              read commands from a server")
    print ("                              instead of a file")
    print ("                              use -c or -i, not both")
    print ("  -i  | --input commands_file /dev/stdin for example")
    print ("  [-o | --output output_file] log to a file instead of stdout")
    print ("  [-p | --post python_module] specify a post processing module")
    print ("                              (omit the '.py' extension)")
    print ("  [-s | --server]             BETA/EXPERIMENTAL feature")
    print ("                              accept remote workers")
    print ("  [-v | --verbose]            enables progress bar")
    print ("  [-w | --workers n]          local worker threads (default is " +
           str(get_nb_procs()) + ")")
    print ("                              must have: n >= 0")
    print ("                              n == 0 can be useful to only run")
    print ("                                     the server")
    sys.exit(0)

cmd_start_tag = "cmd("
res_start_tag = "):res("

# return cmd and its output as a parsable string like:
# cmd("cat myfile"):res("myfile_content")
def parsable_echo((cmd, cmd_out)):
    return cmd_start_tag + cmd + res_start_tag + cmd_out + ")"

# this is a stupid parser, not taking into account parenthesis nesting
def parse_cmd_echo(cmd_and_output):
    cmd = ""
    cmd_out = ""
    if cmd_and_output.find(cmd_start_tag) == 0:
        res_tag_index = cmd_and_output.find(res_start_tag)
        cmd = cmd_and_output[len(cmd_start_tag):res_tag_index]
        cmd_out = cmd_and_output[res_tag_index + len(res_start_tag):
                                 len(cmd_and_output) - 1]
    else:
        cmd_out = cmd_and_output
    return (cmd, cmd_out)

def get_nb_procs():
    return int(commands.getoutput("egrep -c '^processor' /proc/cpuinfo"))

def worker_wrapper(master, lock):
    try:
        work = master.get_work()
        while work != "":
            cmd_out = commands.getoutput(work)
            master.put_result((work, cmd_out))
            work = master.get_work()
    except ConnectionClosedError: # server closed because no more jobs to send
        pass
    lock.release()

# a pair parameter is required by start_new_thread,
# hence the unused '_' parameter
def master_wrapper(daemon, _):
    # start infinite loop
    print 'Master started'
    daemon.requestLoop()

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
        show_progress      = False
        output_to_file     = False
        commands_file      = None
        output_file        = None
        post_proc_fun      = None
        daemon             = None
        args               = sys.argv
        local_server_port  = -1
        remote_server_port = -1
        remote_server_name = ""
        nb_threads         = get_nb_procs()
        output_param = first_index_lst(["-o","--output"], args)
        if output_param != -1:
            output_to_file = True
            output_file_param = args[output_param + 1]
            output_file       = open(output_file_param, 'a')
        input_param = first_index_lst(["-i","--input"], args)
        remote_server_param = first_index_lst(["-c","--client"], args)
        if input_param != -1:  # mandatory option
            commands_file_param = args[input_param + 1]
            commands_file  = open(commands_file_param, 'r')
        elif remote_server_param == -1:
            print "-i or -c is mandatory"
            usage() # -h | --help falls here also
        if first_index_lst(["-v","--verbose"], args) != -1:
            show_progress = True
        nb_workers_param = first_index_lst(["-w","--workers"], args)
        if nb_workers_param != -1:
            nb_threads = int(args[nb_workers_param + 1])
            if nb_threads < 0:
                usage()
        post_proc_param = first_index_lst(["-p","--post"], args)
        if post_proc_param != -1:
            module = __import__(args[post_proc_param + 1])
            post_proc_fun = module.post_proc
        local_server_param = first_index_lst(["-s","--server"], args)
        if local_server_param != -1:
#             local_server_port = int(args[local_server_param + 1])
            local_server_port = 0
        if remote_server_param != -1:
#             remote_server_name = args[remote_server_param + 1]
#             remote_server_port = int(args[remote_server_param + 2])
            remote_server_name = "server"
            remote_server_port = 0
#             print ("connecting to " + remote_server_name + ':' +
#                    str(remote_server_port))
        # check options coherency
        if input_param != -1 and remote_server_param != -1:
            print "error: -c and -i are exclusive"
            usage()
        commands_queue = Queue()
        results_queue  = Queue()
        master = Master(commands_queue, results_queue)
        nb_jobs        = 0
        locks          = []
        if local_server_port != -1:
            print os.system("pyro-ns & ") # start nameserver (NS)
            time.sleep(6) # wait for NS to start
            Pyro.core.initServer()
            daemon = Pyro.core.Daemon()
            print 'Locating Name Server...'
            locator = Pyro.naming.NameServerLocator()
            ns = locator.getNS()
            daemon.useNameServer(ns)
            # connect a new object (unregister previous one first)
            try:
                # 'master' is our outside world name
                ns.unregister('master')
            except NamingError:
                pass
            # publish master object
            daemon.connect(master,'master')
            thread.start_new_thread(master_wrapper, (daemon, None))
        if input_param != -1:
            # read jobs from local file
            for cmd in commands_file:
                master.add_job(string.strip(cmd))
                nb_jobs += 1
        if remote_server_port != -1 and remote_server_name != "":
            print 'Locating Name Server...'
            locator = Pyro.naming.NameServerLocator()
            ns = locator.getNS()
            try:
                print 'Locating master...'
                URI = ns.resolve('master')
                print 'URI:',URI
            except NamingError,x:
                print "Couldn't find object, nameserver says:",x
                raise SystemExit
            # replace master by its proxy for the remote object
            master = Pyro.core.getProxyForURI(URI)
        for i in range(nb_threads):
            l = thread.allocate_lock()
            l.acquire()
            locks.append(l)
            thread.start_new_thread(worker_wrapper, (master, l))
        if input_param != -1:
            progress_bar = ProgressBar(0, nb_jobs, 60)
            # output everything
            jobs_done = 0
            if show_progress:
                progress_bar.draw()
            while jobs_done < nb_jobs:
                cmd_and_output = results_queue.get()
                jobs_done += 1
                if output_to_file:
                    if post_proc_fun == None:
                        output_file.write(parsable_echo(cmd_and_output) + '\n')
                    else:
                        output_file.write(post_proc_fun(cmd_and_output) + '\n')
                if show_progress:
                    progress_bar.updateAmount(jobs_done)
                    progress_bar.draw()
                elif not output_to_file:
                    if post_proc_fun == None:
                        print parsable_echo(cmd_and_output)
                    else:
                        print post_proc_fun(cmd_and_output)
            # close all files
            commands_file.close()
            if output_to_file:
                output_file.close()
        # wait for everybody
        for l in locks:
            l.acquire()
        # stop pyro server-side stuff
        if daemon != None:
            daemon.disconnect(master)
            daemon.shutdown()
    except SystemExit:
        pass
    except: # unexpected one
        print "exception: ", sys.exc_info()[0]
