#!/usr/bin/env python

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
"""

import commands, logging, os, sys, time

import Pyro.core, Pyro.naming

from tempfile        import TemporaryFile

from MetaDataManager import MetaDataManager
from DataManager     import DataManager
from Pyro.errors     import NamingError

#pyro_default_port = 7766
data_objects_port  = 7767

def launch_data_objects(debug = False):
    Pyro.core.initServer()
    daemon = Pyro.core.Daemon(port = data_objects_port)
    mdm    = MetaDataManager()
    dm     = DataManager()
    daemon.connect(mdm, 'meta_data_manager') # publish them
    daemon.connect(dm,  'data_manager')
    if not debug:
        logfile = open("/tmp/log_dfs_" + os.getenv("USER"), 'wb')
        os.dup2(logfile.fileno(), sys.stdout.fileno())
        os.dup2(logfile.fileno(), sys.stderr.fileno())
        #os.setsid() # make deamons harder to kill by accident
    sys.stdin.close()
    daemon.requestLoop(condition=lambda: dm.pyro_daemon_loop_cond)
    # the following is executed only after dm.stop() was called
    daemon.disconnect(dm)
    daemon.disconnect(mdm)
    daemon.shutdown()
    sys.exit(0)

def usage():
    print """usage:

    DataManager.py [-i] [-h remote_mdm_host[:port]] [command, ...]
      -i : interactive mode
      -h : use remote MetaDataManager

    Basic commands:
    ---------------
    put    local_file [dfs_name]    - publish a file (chunk's md5sums are
                                      computed and published, they are verified
                                      when you get chunks over the network)
    mput   local_dir  [dfs_dir]     - multiple put (recursive)
    cat    dfs_name                 - output file to screen
    app    dfs_name   local_file    - append file to a local one
    get    dfs_name   [local_file]  - retrieve a file
    mget   dfs_dir    [local_dir]   - retrieve a directory
    h[elp]                          - the prose you are reading
    lmdm                            - use the local MetaDataManager (default)
    rmdm host [port]                - use a remote MetaDataManager
    ls                              - list files
    q[uit] | e[xit]                 - stop this wonderful program
    k[ill]                          - stop local data deamons then quit
                                      (DataManager and MetaDataManager)

    Advanced commands:
    ------------------
    uput  local_file [dfs_name]     - unsafe put (no checksums)
    umput local_dir  [dfs_dir]      - unsafe multiple put
    peek  dfs_name   [local_file]   - retrieve a file but don't publish that
                                      you have downloaded its chunks
                                      (selfish get)
    mpeek dfs_name   [local_file]   - selfish mget
    !COMMAND                        - execute local shell command COMMAND

    nget  local_dir  dfs_dir   n    - node get: local put then remote get
                                      from node n (nodes must use same MDM)
    nmget local_dir  [dfs_dir] n    - node mget, local mput then remote mget
                                      from node n (nodes must use same MDM)
    +c                              - add data checksums on put
    -c                              - no checksums on put (default)
    +z                              - add compression on put
                                      NOT IMPLEMENTED
    -z                              - no compression on put (default)
                                      NOT IMPLEMENTED
    pm                              - display current put mode

    Hacker commands:
    ----------------
    dlc                             - describe local chunks
    lsac                            - list all chunks
    lsacs                           - list all chunk and checksums
    lslc                            - list local chunks
    lsn                             - list nodes holding chunks
    """

def process_commands(commands_list, dm, interactive):
    splitted = commands_list.split()
    argc     = len(splitted)
    command  = splitted[0]
    param_1  = None
    param_2  = None
    param_3  = None
    if argc >= 2:
        param_1 = splitted[1]
    if argc >= 3:
        param_2 = splitted[2]
    if argc >= 4:
        param_3 = splitted[3]
    if commands_list.startswith('!'): # run local shell command
        print commands.getoutput(commands_list[1:])
    elif command in ["help", "h"]:
        usage()
    elif command == "lmdm":
        dm.use_local_mdm()
        print "connected to local MDM"
    elif command == "pm":
        if dm.check():
            print "checksums   ON"
        else:
            print "checksums   OFF"
        if dm.comp():
            print "compression ON"
        else:
            print "compression OFF"
    elif command == "+c":
        dm.use_checksums()
        print "add checksum on put"
    elif command == "-c":
        dm.dont_use_checksums()
        print "no checksum on put"
    elif command == "+z":
        dm.use_compression()
        print "compression on put"
    elif command == "-z":
        dm.dont_use_compression()
        print "no compression on put"
    elif command == "rmdm":
        try:
            rmdm_OK = False
            if argc == 2:
                rmdm_OK = dm.use_remote_mdm(param_1)
            elif argc == 3:
                rmdm_OK = dm.use_remote_mdm(param_1, param_2)
            else:
                logging.error("need one or two params")
            if rmdm_OK:
                print "connected to remote MDM"
            else:
                if argc == 2:
                    logging.error("MDM not running on: " +
                                  param_1)
                if argc == 3:
                    logging.error("MDM not running on: " +
                                  param_1 + ":" + param_2)
        except Pyro.errors.URIError:
            logging.error("unknown host: " + param_1)
    elif command == "ls":
        print "files:"
        for f in dm.ls_files():
            print "  " + f
    elif command == "lsac":
        print "all chunks:"
        for c in dm.ls_all_chunks():
            print "  " + c
    elif command == "dlc":
        print "local chunk descriptions:"
        print dm.desc_local_chunks()
    elif command == "lsacs":
        print "all chunk and checksums:"
        for l in dm.ls_all_chunk_and_sums():
            for (c, s) in l:
                if s: print "  " + s + ':' + c
                else: print "  _:" + c
    elif command == "lslc":
        print "local chunks:"
        for c in dm.ls_local_chunks():
            print "  " + c
    elif command == "lsn":
        print "nodes:"
        for n in dm.ls_nodes():
            print "  " + n
    elif command in ["k","kill"]:
        dm.stop_local_mdm()
        dm.stop()
        print "kill: command sent to local deamons"
        sys.exit(0)
    elif command in ["put", "uput"]:
        if argc not in [2, 3]:
            logging.error("need one or two params")
        else:
            if command == "uput":
                dm.put(param_1, param_2)
            else: # put
                dm.put(param_1, param_2)
    elif command in ["nget","nmget"]:
        if argc != 4:
            logging.error("need three params")
        else:
            rdm_URI  = ("PYROLOC://" + param_3 + ":" +
                        str(data_objects_port) + "/data_manager")
            rdm      = None
            try:
                rdm  = Pyro.core.getProxyForURI(rdm_URI)
            except Pyro.errors.URIError:
                logging.error("unknown host: " + param_3)
            rdm_started  = False
            try:
                rdm_started  = rdm.started()
            except:
                pass
            if not rdm_started:
                logging.error("DM not running on: " + param_3)
            else:
                if command == "nget":
                    dm.put(param_1, param_2)
                    rdm.get(param_2, param_1)
                else: # nmget
                    dm.mput(param_1, param_2)
                    rdm.mget(param_2, param_1)
    elif command in ["mput", "umput"]:
        if argc not in [2, 3]:
            logging.error("need one or two params")
        else:
            if command == "umput":
                dm.mput(param_1, param_2)
            else: # mput
                dm.mput(param_1, param_2)
    elif command == "get":
        if argc not in [2, 3]:
            logging.error("need one or two params")
        else:
            dm.get(param_1, param_2)
    elif command == "mget":
        if argc not in [2, 3]:
            logging.error("need one or two params")
        else:
            dm.mget(param_1, param_2)
    elif command == "peek":
        if argc not in [2, 3]:
            logging.error("need one or two params")
        else:
            dm.get(param_1, param_2, False, True)
    elif command == "mpeek":
        if argc not in [2, 3]:
            logging.error("need one or two params")
        else:
            dm.mget(param_1, param_2, True)
    elif command == "app":
        if argc not in [3]:
            logging.error("need two params")
        else:
            dm.get(param_1, param_2, True)
    elif command == "cat":
        if argc not in [2]:
            logging.error("need one param")
        else:
            dm.get(param_1, "/dev/stdout")
    elif command in ["q","quit", "e", "exit"]:
        sys.exit(0)
    else:
        logging.error("unknown command: " + command)
        if interactive: usage()

# index of value v in list l, -1 if not found
def find(v, l):
    try:
        return l.index(v)
    except:
        return -1

# host[:port] -> (host, None|port_int)
def decode_host_maybe_port(host_maybe_port):
    port     = None
    splitted = host_maybe_port.split(':')
    host     = splitted[0]
    try:
        port = int(splitted[1])
    except:
        pass
    return (host, port)

if __name__ == '__main__':
    logging.basicConfig(level  = logging.DEBUG,
                        format = '%(asctime)s %(levelname)s %(message)s')
    #print "WARNING: this tool is EXPERIMENTAL..."
    commands_start = 1
    debug          = False
    interactive    = False
    remote_mdm     = False
    mdm_host       = "localhost"
    mdm_port       = data_objects_port
    remote_mdm_i   = find("-h", sys.argv)
    if remote_mdm_i != -1:
        remote_mdm = True
        remote_mdm_maybe_port = sys.argv[remote_mdm_i + 1]
        sys.argv.pop(remote_mdm_i) # -h
        sys.argv.pop(remote_mdm_i) # host[:port]
        (host, maybe_port) = decode_host_maybe_port(remote_mdm_maybe_port)
        mdm_host = host
        if maybe_port:
            mdm_port = maybe_port
    if "-i" in sys.argv:
        interactive = True
    else:
        commands = " ".join(sys.argv[commands_start:])
    dm_URI  = ("PYROLOC://localhost:" + str(data_objects_port) +
               "/data_manager")
    #print dm_URI
    dm = Pyro.core.getProxyForURI(dm_URI)
    dm_already_here = False
    try:
        dm_already_here = dm.started()
    except Pyro.errors.ProtocolError: # no local DM running
        print "new DM and MDM"
        pid = os.fork()
        if pid == 0: # child process
            launch_data_objects(debug)
    if not dm_already_here:
        time.sleep(0.1) # wait for them to enter the infinite loop
                        # FBR: unneeded latency?
        dm.use_remote_mdm("localhost", data_objects_port)
    if remote_mdm:
        dm.use_remote_mdm(mdm_host, mdm_port)
    if interactive:
        try:
            usage()
            sys.stdout.write("dfs# ") # a cool prompt, isn't it? :-)
            read = sys.stdin.readline().strip()
            while len(read) > 0:
                process_commands(read, dm, interactive)
                sys.stdout.write("dfs# ") # a cool prompt, isn't it? :-)
                read = sys.stdin.readline().strip()
        except KeyboardInterrupt:
            pass
    else:
        if len(commands) == 0:
            usage()
        else:
            for c in commands.split(','):
                process_commands(c, dm, interactive)
