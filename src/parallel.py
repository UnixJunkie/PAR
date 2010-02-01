#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Copyright (C) 2009, 2010, Zhang Initiative Research Unit,
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

import commands, os, socket, sys, time, thread
import Pyro.core, Pyro.naming

from optparse    import OptionParser
from threading   import Thread

from Queue       import Queue, Empty
from ProgressBar import ProgressBar
from Pyro.errors import PyroError, NamingError, ConnectionClosedError
from StringIO    import StringIO

from MetaDataManager import MetaDataManager

class Master(Pyro.core.ObjBase):
    def __init__(self, commands_q, results_q):
        Pyro.core.ObjBase.__init__(self)
        self.jobs_queue     = commands_q
        self.results_queue  = results_q
        self.lock           = thread.allocate_lock()
        self.no_more_jobs   = False

    def get_work(self, previous_result = None):
        if previous_result != None:
            self.results_queue.put(previous_result)
        res = None
        self.lock.acquire()
        if self.no_more_jobs:
            res = ""
        else:
            res = self.jobs_queue.get(True)
            if res == "END":
                self.no_more_jobs = True
        self.lock.release()
        return res

    def add_job(self, cmd):
        self.jobs_queue.put(cmd)

def get_nb_procs():
    res = None
    try:
        # Linux
        res = int(commands.getoutput("egrep -c '^processor' /proc/cpuinfo"))
    except:
        try:
            # {Free|Net|Open}BSD and MacOS X
            res = int(commands.getoutput("sysctl -n hw.ncpu"))
        except:
            res = 0
    return res

def worker_wrapper(master, lock):
    try:
        work = master.get_work()
        while work != "":
            # there is a bug in StringIO, calling getvalue on one where it
            # was never written throws an exception instead of returning an
            # empty string
            cmd_out = StringIO()
            cmd_out.write("i:" + work)
            stdin, stdout, stderr = os.popen3(work)
            stdin.close()
            for l in stdout:
                cmd_out.write("o:" + l)
            stdout.close()
            for l in stderr:
                cmd_out.write("e:" + l)
            stderr.close()
            in_out_err = cmd_out.getvalue()
            cmd_out.close()
            # FBR: compression hook should be here
            #      this could be pretty big stuff to send
            work = master.get_work(in_out_err)
    except ConnectionClosedError: # server closed because no more jobs to send
        pass
    #print "no more jobs for me, leaving"
    lock.release()

class NameServerThread(Thread):

   def __init__ (self, starter):
      Thread.__init__(self, name = "NameServerThread")
      self.starter = starter
      self.setDaemon(True) # Python will exit and clean correctly once
                           # only daemon threads are still running

   # Pyro nameserver thread setup and start
   def run(self):
       # Options and behavior we want:
       # -x: no broadcast listener
       # -m: several nameservers on same network OK
       # -r: don't try to find any other nameserver
       # cf. Pyro-3.10/Pyro/naming.py if you need to change/understand
       # code below
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
           self.starter.start(host, port, bcport, keep, persistent,
                              dbdir, Guards, allowmultiple, dontlookupother,
                              verbose, role = (role, roleArgs),
                              bcaddr = bcaddr, nobroadcast = nobroadcast)
       except (Pyro.errors.NamingError, Pyro.errors.DaemonError),x:
           print "error while starting Pyro nameserver:" + x
           sys.exit(1)

pyro_daemon_loop_cond = True

# a pair parameter is required by start_new_thread,
# hence the unused '_' parameter
def master_wrapper(daemon, _):
    # start infinite loop
    print 'Master started'
    daemon.requestLoop(condition=lambda: pyro_daemon_loop_cond)

optparse_usage = """Usage: %prog [options] {-i | -c} ...
Execute commands in a parallel and/or distributed way."""

my_parser = OptionParser(usage = optparse_usage)
my_parser.add_option("-c", "--client",
                     dest = "server_name", default = None,
                     help = ("read commands from a server instead of a file "
                             "(incompatible with -i)"))
my_parser.add_option("-d", "--dfs",
                     action="store_true",
                     dest = "data_server", default = False,
                     help = ("EXPERIMENTAL: allow distributed "
                             "filesystem capability"))
my_parser.add_option("-i", "--input",
                     dest = "commands_file", default = None,
                     help = ("/dev/stdin for example "
                             "(incompatible with -c)"))
my_parser.add_option("-o", "--output",
                     dest = "output_file", default = None,
                     help = "log to a file instead of stdout")
my_parser.add_option("-p", "--post",
                     dest = "post_proc", default = None,
                     help = ("specify a Python post processing module "
                             "(omit the '.py' extension)"))
my_parser.add_option("-s", "--server",
                     action="store_true",
                     dest = "is_server", default = False,
                     help = "accept remote workers")
my_parser.add_option("-v", "--verbose",
                     action="store_true",
                     dest = "is_verbose", default = False,
                     help = "enable progress bar")
my_parser.add_option("-w", "--workers",
                     dest = "nb_local_workers", default = None,
                     help = ("number of local worker threads, "
                             "must be >= 0, "
                             "default is number of detected cores, very "
                             "probably 0 if your OS is not Linux"))

def usage():
    my_parser.print_help()
    sys.exit(0)

default_pyro_ns_port = 9090

if __name__ == '__main__':
    try:
        (options, optargs)    = my_parser.parse_args()
        show_progress         = options.is_verbose
        commands_file_option  = options.commands_file
        read_from_file        = commands_file_option != None
        output_file_option    = options.output_file
        output_to_file        = output_file_option != None
        remote_server_name    = options.server_name
        connect_to_server     = remote_server_name != None
        nb_workers            = options.nb_local_workers
        nb_threads            = get_nb_procs() # automatic detection
        has_nb_workers_option = nb_workers != None
        post_proc_option      = options.post_proc
        post_proc_fun         = None
        has_post_proc_option  = post_proc_option != None
        is_server             = options.is_server
        daemon                = None
        has_data_server       = options.data_server
        meta_data_manager     = None
        if output_to_file:
            output_file = open(output_file_option, 'a')
        if read_from_file:  # mandatory option
            commands_file  = open(commands_file_option, 'r')
        elif not connect_to_server:
            print "-i or -c is mandatory"
            usage() # -h | --help falls here also
        if has_nb_workers_option:
            nb_threads = int(nb_workers)
            if nb_threads < 0:
                usage()
        elif nb_threads <= 0:
            print ("fatal: unable to find the number of CPU, "
                   "use the -w option")
            usage()
        if has_post_proc_option:
            module = __import__(post_proc_option)
            post_proc_fun = module.post_proc
        # check options coherency
        if read_from_file and connect_to_server:
            print "error: -c and -i are exclusive"
            usage()
        commands_queue = Queue()
        results_queue  = Queue()
        master = Master(commands_queue, results_queue)
        nb_jobs        = 0
        locks          = []
        if is_server:
            starter    = Pyro.naming.NameServerStarter()
            ns_wrapper = NameServerThread(starter)
            ns_wrapper.start()
            while not starter.waitUntilStarted():
                time.sleep(0.1)
            Pyro.core.initServer()
            daemon = Pyro.core.Daemon()
            print 'Locating Name Server...'
            locator = Pyro.naming.NameServerLocator()
            nameserver = locator.getNS(socket.getfqdn(),
                                       port = default_pyro_ns_port)
            print 'Located'
            daemon.useNameServer(nameserver)
            # unpublish previous objects
            for u in ['master', 'meta_data_manager']:
                try:
                    nameserver.unregister(u)
                except NamingError:
                    pass
            # publish objects
            daemon.connect(master, 'master')
            if has_data_server:
                meta_data_manager = MetaDataManager()
                daemon.connect(meta_data_manager, 'meta_data_manager')
            thread.start_new_thread(master_wrapper, (daemon, None))
        if read_from_file:
            # read jobs from local file
            for cmd in commands_file:
                master.add_job(cmd)
                nb_jobs += 1
            master.add_job("END")
        if connect_to_server:
            print 'Locating Name Server...'
            locator = Pyro.naming.NameServerLocator()
            ns = locator.getNS(host = remote_server_name,
                               port = default_pyro_ns_port)
            print 'Located'
            try:
                print 'Locating master...'
                URI = ns.resolve('master')
                print 'Located'
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
            progress_bar = ProgressBar(0, nb_jobs)
            # output everything
            jobs_done = 0
            if show_progress:
                progress_bar.draw()
            while jobs_done < nb_jobs:
                cmd_and_output = results_queue.get()
                jobs_done += 1
                # FBR: more code factorization possible here
                #      if there is a default post_proc function which
                #      is the identity function
                if output_to_file:
                    if has_post_proc_option:
                        output_file.write(post_proc_fun(cmd_and_output))
                    else:
                        output_file.write(cmd_and_output)
                if show_progress:
                    progress_bar.update(jobs_done)
                    progress_bar.draw()
                elif not output_to_file:
                    if has_post_proc_option:
                        sys.stdout.write(post_proc_fun(cmd_and_output))
                    else:
                        sys.stdout.write(cmd_and_output)
            # cleanup
            pyro_daemon_loop_cond = False
            commands_file.close()
            if output_to_file:
                output_file.close()
        # wait for everybody
        for l in locks:
            l.acquire()
        # stop pyro server-side stuff
        if is_server:
            if has_data_server:
                daemon.disconnect(meta_data_manager)
            daemon.disconnect(master)
            daemon.shutdown()
    except SystemExit:
        pass
    except: # unexpected one
        print "exception: ", sys.exc_info()[0]
