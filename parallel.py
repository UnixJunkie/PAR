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
"""

import commands, os, socket, string, sys, time, thread
import Pyro.core, Pyro.naming

from optparse import OptionParser

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
    res = None
    try:
        # Non Linux OS don't have this file
        res = int(commands.getoutput("egrep -c '^processor' /proc/cpuinfo"))
    except:
        res = 0
    return res

def worker_wrapper(master, lock):
    try:
        work = master.get_work()
        while work != "":
            cmd_out = commands.getoutput(work)
            master.put_result((work, cmd_out))
            work = master.get_work()
    except ConnectionClosedError: # server closed because no more jobs to send
        pass
    print "no more jobs for me, leaving"
    lock.release()

# Pyro nameserver thread setup and start
# a pair parameter is required by start_new_thread,
# hence the unused '_' parameter
def nameserver_wrapper(starter, _):
    # Options and behavior we want:
    # -x: no broadcast listener
    # -m: several nameservers on same network OK
    # -r: don't try to find any other nameserver
    # cf. Pyro-3.10/Pyro/naming.py if you need to change/understand code below
    host = None
    port = None
    if port:
        port = int(port)
    bcport = 0
    bcaddr = None
    nobroadcast = True
    role = Pyro.constants.NSROLE_SINGLE
    roleArgs = None
    verbose = False
    keep = False
    allowmultiple = True
    dontlookupother = True
    persistent = 0
    dbdir = None
    Guards = (None,None)
    print '*** Starting Pyro Name Server ***'
    try:
        starter.start(host, port, bcport, keep, persistent, dbdir, Guards,
                      allowmultiple, dontlookupother, verbose,
                      role = (role, roleArgs), bcaddr = bcaddr,
                      nobroadcast = nobroadcast)
    except (Pyro.errors.NamingError, Pyro.errors.DaemonError),x:
        print "error while starting Pyro nameserver:" + x
        pass

pyro_daemon_loop_cond = True

# a pair parameter is required by start_new_thread,
# hence the unused '_' parameter
def master_wrapper(daemon, _):
    # start infinite loop
    print 'Master started'
    daemon.requestLoop(condition=lambda: pyro_daemon_loop_cond)

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

optparse_usage = """Usage: %prog [options] {-i | -c} ...
Execute commands in a parallel and/or distributed way."""

parser = OptionParser(usage = optparse_usage)
parser.add_option("-c", "--client",
                  dest = "server_name", default = None,
                  help = ("read commands from a server instead of a file "
                          "(incompatible with -i)"))
parser.add_option("-i", "--input",
                  dest = "commands_file", default = None,
                  help = ("/dev/stdin for example "
                          "(incompatible with -c)"))
parser.add_option("-o", "--output",
                  dest = "output_file", default = None,
                  help = "log to a file instead of stdout")
parser.add_option("-p", "--post",
                  dest = "post_proc", default = None,
                  help = ("specify a Python post processing module "
                          "(omit the '.py' extension)"))
parser.add_option("-s", "--server",
                  action="store_true",
                  dest = "is_server", default = False,
                  help = "accept remote workers")
parser.add_option("-v", "--verbose",
                  action="store_true",
                  dest = "is_verbose", default = False,
                  help = "enables progress bar")
parser.add_option("-w", "--workers",
                  dest = "nb_local_workers", default = None,
                  help = ("number of local worker threads, "
                          "must be >= 0, "
                          "default is number of detected cores, very probably "
                          "0 if your OS is not Linux"))

def usage():
    parser.print_help()
    sys.exit(0)

default_pyro_ns_port = 9090

if __name__ == '__main__':
    try:
        (options, optargs)  = parser.parse_args()
        show_progress       = options.is_verbose
        commands_file_param = options.commands_file
        read_from_file      = commands_file_param != None
        output_file_param   = options.output_file
        output_to_file      = output_file_param != None
        remote_server_name  = options.server_name
        connect_to_server   = remote_server_name != None
        post_proc_fun       = options.post_proc
        daemon              = None
        args                = sys.argv
        local_server_port   = -1
        nb_threads          = get_nb_procs()
        # FBR: verify mandatory options are present
        #      verify options coherency, do it here instead of having
        #      it spread all over the place
        if output_to_file:
            output_file = open(output_file_param, 'a')
        remote_server_param = first_index_lst(["-c","--client"], args)
        if read_from_file:  # mandatory option
            commands_file  = open(commands_file_param, 'r')
        elif not connect_to_server:
            print "-i or -c is mandatory"
            usage() # -h | --help falls here also
        if first_index_lst(["-v","--verbose"], args) != -1:
            show_progress = True
        nb_workers_param = first_index_lst(["-w","--workers"], args)
        if nb_workers_param != -1:
            nb_threads = int(args[nb_workers_param + 1])
            if nb_threads < 0:
                usage()
        elif nb_threads == 0:
            print ("fatal: unable to find the number of CPU, "
                   "use the -w option")
            usage()
        post_proc_param = first_index_lst(["-p","--post"], args)
        if post_proc_param != -1:
            module = __import__(args[post_proc_param + 1])
            post_proc_fun = module.post_proc
        local_server_param = first_index_lst(["-s","--server"], args)
        if local_server_param != -1:
            # could be used later on to change Pyro NS port we start
#             local_server_port = int(args[local_server_param + 1])
            local_server_port = 0
        # check options coherency
        if read_from_file and connect_to_server:
            print "error: -c and -i are exclusive"
            usage()
        commands_queue = Queue()
        results_queue  = Queue()
        master = Master(commands_queue, results_queue)
        nb_jobs        = 0
        locks          = []
        if local_server_port != -1:
            starter = Pyro.naming.NameServerStarter()
            thread.start_new_thread(nameserver_wrapper, (starter, None))
            while not starter.waitUntilStarted():
                time.sleep(0.1)
            Pyro.core.initServer()
            daemon = Pyro.core.Daemon()
            print 'Locating Name Server...'
            locator = Pyro.naming.NameServerLocator()
            nameserver = locator.getNS(socket.getfqdn(),
                                       port = default_pyro_ns_port)
            daemon.useNameServer(nameserver)
            # connect a new object (unregister previous one first)
            try:
                # 'master' is our outside world name
                nameserver.unregister('master')
            except NamingError:
                pass
            # publish master object
            daemon.connect(master,'master')
            thread.start_new_thread(master_wrapper, (daemon, None))
        if read_from_file:
            # read jobs from local file
            for cmd in commands_file:
                master.add_job(string.strip(cmd))
                nb_jobs += 1
        if connect_to_server:
            print 'Locating Name Server...'
            locator = Pyro.naming.NameServerLocator()
            ns = locator.getNS(host = remote_server_name,
                               port = default_pyro_ns_port)
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
        if read_from_file:
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
            # cleanup
            pyro_daemon_loop_cond = False
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
