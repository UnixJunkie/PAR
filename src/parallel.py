#!/usr/bin/env python
# must use python 2.4 or higher

"""
If you use and like our software, please send us a postcard! ^^

Copyright (C) 2009, 2010, Zhang Initiative Research Unit,
Advance Science Institute, Riken
2-1 Hirosawa, Wako, Saitama 351-0198, Japan
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

import commands, os, socket, subprocess, sys, tempfile, time, thread
import Pyro.core, Pyro.naming

from optparse    import OptionParser
from threading   import Thread

from Queue       import Queue, Empty
from ProgressBar import ProgressBar
from Pyro.errors import PyroError, NamingError, ConnectionClosedError
from StringIO    import StringIO
from subprocess  import Popen

class Master(Pyro.core.ObjBase):

    def __init__(self, commands_q, results_q, begin_cmd = "", end_cmd = ""):
        Pyro.core.ObjBase.__init__(self)
        self.jobs_queue     = commands_q
        self.results_queue  = results_q
        self.lock           = thread.allocate_lock()
        self.no_more_jobs   = False
        self.begin_command  = begin_cmd
        self.end_command    = end_cmd

    def get_work(self, previous_result = None):
        if previous_result:
            self.results_queue.put(previous_result)
        res = None
        self.lock.acquire()
        if self.no_more_jobs:
            res = ""
        else:
            res = self.jobs_queue.get(True)
            if res == "END":
                self.no_more_jobs = True
                res = ""
        self.lock.release()
        return res

    def add_job(self, cmd):
        self.jobs_queue.put(cmd)

    def get_begin_end_commands(self):
        return (self.begin_command, self.end_command)

def get_nb_procs():
    res = None
    try:
        # POSIX
        res = os.sysconf('SC_NPROCESSORS_ONLN')
    except:
        try:
            # {Free|Net|Open}BSD and MacOS X
            res = int(commands.getoutput("sysctl -n hw.ncpu"))
        except:
            res = 0
    return res

def worker_wrapper(master, lock):
    begin_cmd = ""
    end_cmd   = ""
    try:
        not_started = True
        while not_started:
            try:
                work = master.get_work()
                not_started = False
            except Pyro.errors.ProtocolError:
                print "warning: retrying master.get_work()"
                time.sleep(0.1)
        (begin_cmd, end_cmd) = master.get_begin_end_commands()
        if begin_cmd != "":
            print "worker start: %s" % commands.getoutput(begin_cmd)
        while work != "":
            # there is a bug in StringIO, calling getvalue on one where it
            # was never written throws an exception instead of returning an
            # empty string
            cmd_out = StringIO()
            cmd_out.write("i:%s" % work)
            cmd_stdout = tempfile.TemporaryFile()
            cmd_stderr = tempfile.TemporaryFile()
            p = Popen(work, shell=True, stdout=cmd_stdout, stderr=cmd_stderr,
                      close_fds=True)
            p.wait() # wait for the command to complete
            # rewind its stdout and stderr files
            cmd_stdout.seek(0)
            cmd_stderr.seek(0)
            for l in cmd_stdout:
                cmd_out.write("o:%s" % l)
            cmd_stdout.close()
            for l in cmd_stderr:
                cmd_out.write("e:%s" % l)
            cmd_stderr.close()
            in_out_err = cmd_out.getvalue()
            cmd_out.close()
            # FBR: compression hook should be here
            #      this could be pretty big stuff to send
            work = master.get_work(in_out_err)
    except ConnectionClosedError: # server closed because no more jobs to send
        pass
    #print "no more jobs for me, leaving"
    if end_cmd != "":
        print "worker stop: %s" % commands.getoutput(end_cmd)
    lock.release()

default_pyro_port     = 7766
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
my_parser.add_option("-b", "--begin",
                     dest   = "begin_command", default = "",
                     help   = ("command run by a worker before any job "
                               "(englobe command in parenthesis)"))
my_parser.add_option("-c", "--client",
                     dest = "server_name", default = None,
                     help = ("read commands from a server instead of a file "
                             "(incompatible with -i)"))
my_parser.add_option("-d", "--demux",
                     dest = "demuxer", default = None,
                     help = "specify a demuxer, NOT IMPLEMENTED")
my_parser.add_option("-e", "--end",
                     dest = "end_command", default = "",
                     help = "command run by a worker after last job "
                            "(englobe command in parenthesis)")
my_parser.add_option("-i", "--input",
                     dest = "commands_file", default = None,
                     help = ("/dev/stdin for example "
                             "(incompatible with -c)"))
my_parser.add_option("-m", "--mux",
                     dest = "muxer", default = None,
                     help = "specify a muxer, NOT IMPLEMENTED")
my_parser.add_option("-o", "--output",
                     dest = "output_file", default = None,
                     help = "log to a file instead of stdout")
my_parser.add_option("-p", "--port",
                     dest = "server_port", default = default_pyro_port,
                     help = ("use a specific port number instead of Pyro's "
                             "default one (useful in case of firewall or "
                             "to have several independant servers running "
                             "on the same host computer)"))
my_parser.add_option("--post-proc",
                     dest = "post_proc", default = None,
                     help = ("specify a Python post processing module "
                             "(omit the '.py' extension)"))
my_parser.add_option("-s", "--server",
                     action = "store_true",
                     dest   = "is_server", default = False,
                     help   = "accept remote workers")
my_parser.add_option("-v", "--verbose",
                     action = "store_true",
                     dest   = "is_verbose", default = False,
                     help   = "enable progress bar")
my_parser.add_option("-w", "--workers",
                     dest = "nb_local_workers", default = None,
                     help = ("number of local worker threads, "
                             "must be >= 0, "
                             "default is number of detected cores, very "
                             "probably 0 if your OS is not Linux"))

def usage():
    my_parser.print_help()
    sys.exit(0)

if __name__ == '__main__':
    try:
        (options, optargs)    = my_parser.parse_args()
        show_progress         = options.is_verbose
        commands_file_option  = options.commands_file
        read_from_file        = commands_file_option
        output_file_option    = options.output_file
        output_to_file        = output_file_option
        remote_server_name    = options.server_name
        connect_to_server     = remote_server_name
        nb_workers            = options.nb_local_workers
        nb_threads            = get_nb_procs() # automatic detection
        has_nb_workers_option = nb_workers
        post_proc_option      = options.post_proc
        post_proc_fun         = None
        has_post_proc_option  = post_proc_option
        is_server             = options.is_server
        muxer                 = options.muxer
        demuxer               = options.demuxer
        daemon                = None
        if output_to_file:
            output_file = open(output_file_option, 'w')
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
        master = Master(commands_queue, results_queue,
                        options.begin_command, options.end_command)
        nb_jobs        = 0
        locks          = []
        if is_server:
            Pyro.core.initServer()
            try:
                daemon = Pyro.core.Daemon(port    = int(options.server_port),
                                          norange = True)
            except Pyro.errors.DaemonError:
                print "error: port already used, probably"
                sys.exit(1)
            # publish objects
            uri = daemon.connect(master, 'master')
            #print uri # debug
            thread.start_new_thread(master_wrapper, (daemon, None))
        if connect_to_server:
            # replace master by its proxy for the remote object
            uri = ("PYROLOC://" + remote_server_name + ":" +
                   str(options.server_port) + "/master")
            #print uri # debug
            master = Pyro.core.getProxyForURI(uri)
        # start workers
        for i in range(nb_threads):
            l = thread.allocate_lock()
            l.acquire()
            locks.append(l)
            time.sleep(0.01) # dirty bug correction:
                             # on multiproc machines, starting threads without
                             # waiting makes Pyro output this sometimes:
                             # "Pyro.errors.ProtocolError: unknown object ID"
                             # It is like if the Pyro daemon is not ready yet
                             # to handle many new client threads...
            thread.start_new_thread(worker_wrapper, (master, l))
        # feed workers
        if read_from_file:
            # read jobs from local file
            for cmd in commands_file:
                master.add_job(cmd)
                nb_jobs += 1
            master.add_job("END")
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
            daemon.disconnect(master)
            daemon.shutdown()
    except SystemExit:
        pass
    except: # unexpected one
        print "exception: ", sys.exc_info()[0]
