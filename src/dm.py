#!/usr/bin/python

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

import logging, os, random, socket, sys, stat, thread, time

import Pyro.core, Pyro.naming

from tarfile         import TarFile
from tempfile        import TemporaryFile

from MetaDataManager import MetaDataManager
from DataManager     import DataManager
from Pyro.errors     import NamingError

#pyro_default_port     = 7766
data_manager_port      = 7767
meta_data_manager_port = 7768

def launch_local_meta_data_manager(debug = False):
    Pyro.core.initServer()
    daemon = Pyro.core.Daemon(port = meta_data_manager_port)
    mdm = MetaDataManager()
    daemon.connect(mdm, 'meta_data_manager') # publish object
    if not debug:
        logfile = open("/tmp/mdm_log_dfs_" + os.getlogin(), 'ab')
        os.dup2(logfile.fileno(), sys.stdout.fileno())
        os.dup2(logfile.fileno(), sys.stderr.fileno())
        os.setsid()
    sys.stdin.close()
    daemon.requestLoop(condition=lambda: mdm.pyro_daemon_loop_cond)
    # the following is executed only after mdm.stop() was called
    daemon.disconnect(mdm)
    daemon.shutdown()
    sys.exit(0)

def launch_local_data_manager(debug = False):
    Pyro.core.initServer()
    daemon = Pyro.core.Daemon(port = data_manager_port)
    dm = DataManager()
    daemon.connect(dm, 'data_manager') # publish object
    if not debug:
        logfile = open("/tmp/dm_log_dfs_" + os.getlogin(), 'ab')
        os.dup2(logfile.fileno(), sys.stdout.fileno())
        os.dup2(logfile.fileno(), sys.stderr.fileno())
        os.setsid()
    sys.stdin.close()
    daemon.requestLoop(condition=lambda: dm.pyro_daemon_loop_cond)
    # the following is executed only after dm.stop() was called
    daemon.disconnect(dm)
    daemon.shutdown()
    sys.exit(0)

def usage():
    print """usage:
    DataManager.py [-i] [-h mdm_host[:port]] [command [parameters]]
      -i : interactive mode
      -h : use remote MetaDataManager
    commands:
    ---
    put local_file [dfs_name]   - publish a file
    app dfs_name   local_file   - append file to a local one
    cat dfs_name                - output file to screen
    get dfs_name   [local_file] - retrieve a file
    h[elp]                      - the prose you are reading
    lmdm                        - use the local MetaDataManager (default)
    rmdm host [port]            - use a remote MetaDataManager
    ls                          - list files
    lsac                        - list all chunks
    lslc                        - list local chunks only
    lsn                         - list nodes
    q[uit] | e[xit]             - stop this wonderful program
    k[ill]                      - stop local data deamons then quit
                                  (DataManager and MetaDataManager)
    """

def process_commands(commands, dm, mdm, interactive = False):
    splitted = commands.split()
    argc = len(splitted)
    command = splitted[0]
    param_1 = None
    if argc in [2, 3]:
        param_1 = splitted[1]
    param_2 = None
    if argc == 3:
        param_2 = splitted[2]
    if command in ["help", "h"]:
        usage()
    elif command == "lmdm":
        dm.use_local_mdm()
        print "connected to local MDM"
    elif command == "rmdm":
        try:
            rmdm_OK = False
            if argc == 2:
                rmdm_OK = dm.use_remote_mdm(param_1)
            elif argc == 3:
                rmdm_OK = dm.use_remote_mdm(param_1, param_2)
            else:
                logging.error("need one or two params")
                if interactive: usage()
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
        print dm.ls_files()
    elif command == "lsac":
        print "all chunks:"
        print dm.ls_all_chunks()
    elif command == "lslc":
        print "local chunks:"
        print dm.ls_local_chunks()
    elif command == "lsn":
        print "nodes:"
        print dm.ls_nodes()
    elif command in ["k","kill"]:
        dm.stop()
        mdm.stop()
        sys.exit(0)
    elif command == "put":
        if argc not in [2, 3]:
            logging.error("need one or two params")
            if interactive: usage()
        else:
            dm.put(param_1, param_2)
    elif command == "get":
        if argc not in [2, 3]:
            logging.error("need one or two params")
            if interactive: usage()
        else:
            dm.get(param_1, param_2)
    elif command == "app":
        if argc not in [3]:
            logging.error("need two params")
            if interactive: usage()
        else:
            dm.get(param_1, param_2, True)
    elif command == "cat":
        if argc not in [2]:
            logging.error("need one param")
            if interactive: usage()
        else:
            dm.get(param_1, "/dev/stdout")
    elif command in ["q","quit", "e", "exit"]:
        sys.exit(0)
    else:
        logging.error("unknown command: " + command)
        if interactive: usage()

if __name__ == '__main__':
    logging.basicConfig(level  = logging.DEBUG,
                        format = '%(asctime)s %(levelname)s %(message)s')
    commands_start = 1
    debug          = False
    interactive    = False
    if "-i" in sys.argv:
        interactive = True
    else:
        commands = " ".join(sys.argv[commands_start:])
    dm_URI  = ("PYROLOC://localhost:" + str(data_manager_port) +
               "/data_manager")
    print dm_URI
    mdm_URI = ("PYROLOC://localhost:" + str(meta_data_manager_port) +
               "/meta_data_manager")
    print mdm_URI
    dm  = Pyro.core.getProxyForURI(dm_URI)
    mdm = Pyro.core.getProxyForURI(mdm_URI)
    dm_already_here  = False
    mdm_already_here = False
    try:
        ignore = mdm.started()
        mdm_already_here = True
    except Pyro.errors.ProtocolError:
        # no local MetaDataManager running
        print "starting MDM daemon..."
        pid = os.fork()
        if pid == 0: # child process
            launch_local_meta_data_manager(debug)
    if not mdm_already_here:
        time.sleep(0.1) # wait for him to enter his infinite loop
                        # FBR: I don't like this, it adds some unneeded
                        #      latency
    else:
        print "MDM daemon OK"
    try:
        ignore = dm.started()
        dm_already_here = True
    except Pyro.errors.ProtocolError:
        # no local DataManager running
        print "starting DM  daemon..."
        pid = os.fork()
        if pid == 0: # child process
            launch_local_data_manager(debug)
    if not dm_already_here:
        time.sleep(0.1) # wait for him to enter his infinite loop
    else:
        print "DM  daemon OK"
    if interactive:
        try:
            usage()
            sys.stdout.write("dfs# ") # a cool prompt, isn't it? :-)
            read = sys.stdin.readline().strip()
            while len(read) > 0:
                process_commands(read, dm, mdm, interactive)
                sys.stdout.write("dfs# ") # a cool prompt, isn't it? :-)
                read = sys.stdin.readline().strip()
        except KeyboardInterrupt:
            pass
    else:
        if len(commands) == 0:
            usage()
        else:
            for c in commands.split(','):
                logging.debug(c)
                process_commands(c, dm, mdm)