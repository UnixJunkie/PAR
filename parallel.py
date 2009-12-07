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
 * test this:
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
 * profile this program with the python profiler
 * add a code coverage test, python is not compiled and pychecker is too
   light at ckecking things
 * correct this harmless bug (program should exit cleanly instead of
   throwing an exception):
   user@host# parallel.py -i /dev/null
   exception:  exceptions.ZeroDivisionError
"""

import commands, select, socket, string, sys, thread

from Queue import Queue, Empty
from ProgressBar import ProgressBar

def usage():
    print ("Usage: parallel.py [options] -i | -c ...")
    print ("Execute commands in parallel.")
    print ("")
    print ("  [-h | --help]               you are currently reading it")
    print ("  -c  | --client host port    BETA/EXPERIMENTAL feature")
    print ("                              read commands from a server")
    print ("                              instead of a file")
    print ("                              use -c or -i, not both")
    print ("  -i  | --input commands_file /dev/stdin for example")
    print ("  [-o | --output output_file] log to a file instead of stdout")
    print ("  [-p | --post python_module] specify a post processing module")
    print ("                              (omit the '.py' extension)")
    print ("  [-s | --server port]        BETA/EXPERIMENTAL feature")
    print ("                              accept remote workers")
    print ("  [-v | --verbose]            enables progress bar")
    print ("  [-w | --workers n]          local worker threads (default is " +
           str(get_nb_procs()) + ")")
    print ("                              must have: n >= 0")
    print ("                              n == 0 can be useful to only run")
    print ("                                     the server")
    sys.exit(0)

# return cmd and its output as a parsable string like:
# cmd("cat myfile"):res("myfile_content")
def parsable_echo((cmd, cmd_out)):
    return "cmd(" + cmd + "):res(" + cmd_out + ")"

def get_nb_procs():
    return int(commands.getoutput("egrep -c '^processor' /proc/cpuinfo"))

def local_worker_wrapper(commands_queue, results_queue, lock):
    try:
        while (True):
            cmd = commands_queue.get(True, 1)
            cmd_out = commands.getoutput(cmd)
            results_queue.put((cmd, cmd_out))
    except Empty:
        lock.release()

def receive_all(sock, log_prefix):
    buff = ""
    try:
        receive_size = 4096
        received = sock.recv(receive_size)
        while len(received) > 0:
            buff += received
            received = sock.recv(receive_size)
    except:
        print log_prefix, "server closed or receive timedout"
    return buff

def remote_worker_wrapper(lock, server_name, server_port):
    log_prefix = "network worker:"
    while True:
        try:
            client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_sock.connect((server_name, server_port))
            client_sock.settimeout(5)
        except:
            print log_prefix + "server is down"
            client_sock.close()
            break # end infinite loop, server no more here
        cmd = receive_all(client_sock, log_prefix)
        if cmd == "":
            client_sock.close()
            break # end infinite loop, server no more here
        #print "received:" + cmd
        cmd_out = commands.getoutput(cmd)
        try:
            to_send = parsable_echo((cmd, cmd_out))
            client_sock.sendall(to_send)
            client_sock.shutdown(socket.SHUT_WR)
            #print "sent: " + to_send
        except:
            print log_prefix + "problem while sending result"
        client_sock.close()
    #print "worker left"
    lock.release()

def server_wrapper(commands_queue, results_queue, lock, server_port):
    nb_sent_jobs     = 0
    nb_received_jobs = 0
    # start server
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock_fd = server_sock.fileno()
    server_open = True
    server_name = commands.getoutput("hostname")
    #print "server:", server_name, "port:", str(server_port)
    server_sock.bind((server_name, server_port))
    server_sock.listen(1)
    server_commands = []
    poller = select.poll()
    poller.register(server_sock, select.POLLIN) # new clients to accept
    sock_fd_dict = {} # socket to fd mapping
    while server_open:
        # sorting the poll output would give priorities to the server tasks:
        # accept new client, send job, read result, etc.
        for (fd, event) in poller.poll():
            #print "### poll"
            if fd == server_sock_fd: # accept new client
                if server_open:
                    client_conn, client_addr = server_sock.accept()
                    #print "connection from " + str(client_addr)
                    # he will want a job
                    poller.register(client_conn, select.POLLOUT)
                    sock_fd_dict[client_conn.fileno()] = client_conn
            elif event & select.POLLOUT: # send job
                try:
                    cmd = commands_queue.get(True, 1)
                    server_commands.append(cmd)
                except Empty:
                    #print "server: empty commands queue"
                    if len(server_commands) == 0:
                        #print "server: empty server queue"
                        if server_open and nb_received_jobs == nb_sent_jobs:
                            server_sock.shutdown(socket.SHUT_RDWR)
                            poller.unregister(server_sock)
                            server_sock.close()
                            server_open = False
                if len(server_commands) > 0:
                    cmd = server_commands.pop(0)
                    client_conn = sock_fd_dict[fd]
                    try:
                        client_conn.sendall(cmd)
                        client_conn.shutdown(socket.SHUT_WR)
                        poller.unregister(client_conn)
                        # he will give a result
                        poller.register(client_conn, select.POLLIN)
                        nb_sent_jobs += 1
                        #print "server: sent one job"
                        #print "nb jobs sent: " + str(nb_sent_jobs)
                    except:
                        print "server: problem while sending command"
                        server_commands.insert(0, cmd) # keep for next client
                        poller.unregister(client_conn)
                        client_conn.close() # fire client
            elif event & select.POLLIN: # get result
                client_conn = sock_fd_dict[fd]
                result = receive_all(client_conn, "server:")
                #print "server received:" + result
                poller.unregister(client_conn)
                client_conn.close()
                sock_fd_dict.pop(fd)
                nb_received_jobs += 1
                results_queue.put(("",result)) # FBR: put unparsed result
                #print "server: got one job"
                #print "nb jobs received: " + str(nb_received_jobs)
            else:
                print "server: unhandled event"
                sys.exit(1)
    #print "server: all jobs done"
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
        show_progress      = False
        output_to_file     = False
        commands_file      = None
        output_file        = None
        post_proc_fun      = None
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
            local_server_port = int(args[local_server_param + 1])
        if remote_server_param != -1:
            remote_server_name = args[remote_server_param + 1]
            remote_server_port = int(args[remote_server_param + 2])
            print ("connecting to " + remote_server_name + ':' +
                   str(remote_server_port))
        # check options coherency
        if input_param != -1 and remote_server_param != -1:
            print "error: -c and -i are exclusive"
            usage()
        commands_queue = Queue()
        results_queue  = Queue()
        nb_jobs        = 0
        locks          = []
        if local_server_port != -1:
            l = thread.allocate_lock()
            l.acquire()
            locks.append(l)
            thread.start_new_thread(server_wrapper,
                                    (commands_queue, results_queue, l,
                                     local_server_port))
        if remote_server_port != -1 and remote_server_name != "":
            # start clients of remote server
            for i in range(nb_threads):
                l = thread.allocate_lock()
                l.acquire()
                locks.append(l)
                thread.start_new_thread(remote_worker_wrapper,
                                        (l,
                                         remote_server_name,
                                         remote_server_port))
        else: # start local workers
            for i in range(nb_threads):
                l = thread.allocate_lock()
                l.acquire()
                locks.append(l)
                thread.start_new_thread(local_worker_wrapper,
                                        (commands_queue, results_queue, l))
        if input_param != -1:
            # read jobs from local file
            for cmd in commands_file:
                commands_queue.put(string.strip(cmd))
                nb_jobs += 1
            progress_bar = ProgressBar(0, nb_jobs, 60)
            # output everything
            jobs_done = 0
            if show_progress:
                progress_bar.draw()
            while jobs_done < nb_jobs:
                #print "not finished with commands file"
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
            #print "finished with commands file"
            if output_to_file:
                output_file.close()
        # wait for everybody
        for l in locks:
            l.acquire()
    except SystemExit:
        pass
    except: # unexpected one
        print "exception: ", sys.exc_info()[0]
