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
"""

import commands, logging, os, cPickle, random
import socket, sys, stat, thread, time

import Pyro.core, Pyro.naming

from tarfile         import TarFile
from tempfile        import TemporaryFile

from MetaDataManager import MetaDataManager
from Pyro.errors     import NamingError

# FBR: * logs should go to a local file?
#        Sure but only when not in interactive mode

pyro_default_port      = 7766
data_manager_port      = 7767
meta_data_manager_port = 7768

class DataManager(Pyro.core.ObjBase):

    def __init__(self):
        Pyro.core.ObjBase.__init__(self)
        # when compressing, we must compress before cuting into chunks so we
        # will have fewer chunks to transfer instead of having smaller ones
        # (better for network latency I think)
        ##################################
        # SYNCHRONIZE ACCES TO DATASTORE #
        ##################################
        self.data_store_lock       = thread.allocate_lock()
        self.chunk_server_lock     = thread.allocate_lock()
        self.CHUNK_SIZE            = 1024*1024
        self.data_store            = None
        self.hostname              = socket.getfqdn()
        self.storage_file          = ("/tmp/dfs_" + os.getlogin() +
                                      "_at_" + self.hostname)
        self.local_chunks          = {}
        self.pyro_daemon_loop_cond = True
        try:
            # The TarFile interface forces us to use a temporary file.
            # Maybe it makes some unnecessary data copy, however having
            # our chunks kept in the standard tar format is too cool
            # to be changed
            self.data_store_lock.acquire()
            self.data_store = TarFile(self.storage_file, 'w')
            os.chmod(self.data_store.fileobj.name,
                     stat.S_IRUSR | stat.S_IWUSR) # <=> chmod 600 ...
            self.data_store_lock.release()
            temp_file = TemporaryFile()
            temp_file.write("DFS_STORAGE_v00\n")
            temp_file.flush()
            temp_file.seek(0)
            self.add_local_chunk(0, "STORAGE_INITIALIZED", temp_file)
            temp_file.close()
        except:
            logging.exception("can't create or write to: " + self.storage_file)
            sys.exit(0)
        self.mdm = None
        self.use_local_mdm()

    # change MetaDataManager
    def use_remote_mdm(self, host, port = meta_data_manager_port):
        mdm_URI = "PYROLOC://" + host + ":" + str(port) + "/meta_data_manager"
        self.mdm = Pyro.core.getProxyForURI(mdm_URI)

    # change MetaDataManager (default)
    def use_local_mdm(self):
        self.use_remote_mdm("localhost", meta_data_manager_port)

    def get_chunk_name(self, chunk_number, dfs_path):
        if dfs_path.startswith('/'):
            dfs_path = dfs_path[1:]
        return str(chunk_number) + '/' + dfs_path

    def chunk_name_to_index(self, chunk_name):
        return int((chunk_name.split('/'))[0])

    def add_local_chunk(self, chunk_number, dfs_path, tmp_file):
        chunk_name = self.get_chunk_name(chunk_number, dfs_path)
        self.data_store_lock.acquire()
        tar_info = self.data_store.gettarinfo (arcname = chunk_name,
                                               fileobj = tmp_file)
        self.data_store.addfile(tar_info, tmp_file)
        self.data_store.fileobj.flush()
        self.local_chunks[chunk_name] = True
        self.data_store_lock.release()

    # return (False, None) if busy
    #        (True***,  None) if not busy but chunk was not found
    #                         WARNING: client must update meta data info then?
    #        (True***,  c)    if not busy and chunk found
    # ***IMPORTANT: call got_chunk just after on client side if True was
    #               returned as first in the pair
    def get_chunk(self, chunk_name):
        res = (False, None)
        ready = self.chunk_server_lock.acquire(False)
        if ready:
            # we acquired the lock to forbid simultaneous transfers
            # because we don't want the bandwidth to be shared between clients
            if self.local_chunks.get(chunk_name) == None:
                res = (True, None)
            else:
                self.data_store_lock.acquire()
                read_only_data_store = TarFile(self.storage_file, 'r')
                untared_file = read_only_data_store.extractfile(chunk_name)
                self.data_store_lock.release()
                if untared_file == None:
                    logging.fatal("could not extract " + chunk_name +
                                  " from local store despite it was listed" +
                                  " in self.local_chunks")
                    res = (True, None)
                else:
                    chunk = untared_file.read()
                    res = (True, chunk)
        return res

    # notify transfer finished so that other can download chunks from the
    # current DataManager
    def got_chunk(self):
        self.chunk_server_lock.release()

    def ls_local_chunks(self):
        res = []
        self.data_store_lock.acquire()
        res = self.local_chunks.keys()
        self.data_store_lock.release()
        return res

    # publish a local file into the DFS
    def put(self, filename, dfs_path = None):
        if not os.path.isfile(filename):
            logging.error("no such file: " + filename)
        else:
            if dfs_path == None:
                dfs_path = filename
            # compression of added file hook should be here
            input_file = open(filename, 'rb')
            try:
                file_size = 0
                chunk_index = 0
                read_buff = input_file.read(self.CHUNK_SIZE)
                while read_buff != '':
                    file_size += len(read_buff)
                    temp_file = TemporaryFile()
                    temp_file.write(read_buff)
                    temp_file.flush()
                    temp_file.seek(0)
                    self.add_local_chunk(chunk_index, dfs_path, temp_file)
                    temp_file.close()
                    chunk_index += 1
                    read_buff = input_file.read(self.CHUNK_SIZE)
                self.mdm.publish_meta_data(dfs_path, self.hostname,
                                           file_size, chunk_index)
            except:
                logging.exception("problem while reading " + filename)
            input_file.close()

    # download a DFS file and dump it to a local file
    def get(self, dfs_path, fs_output_path, append_mode = False):
        if fs_output_path == None:
            fs_output_path = dfs_path
        # shuffle is here to increase pipelining and parallelization
        # of transfers
        remote_chunks = []
        meta_info = self.mdm.get_meta_data(dfs_path)
        if meta_info == None:
            logging.error("no such file: " + dfs_path)
        else:
            self.data_store_lock.acquire()
            for c in meta_info.get_chunk_names():
                if self.local_chunks.get(c) == None:
                    remote_chunks.append(c)
            self.data_store_lock.release()
            random.shuffle(remote_chunks)
            # FBR: TODO download them then publish current host as hosting
            #      this chunks too
            #
            # dump all chunks from local store to fs_output_path and
            # in the right order please
            if append_mode:
                output_file = open(fs_output_path, 'ab')
            else:
                output_file = open(fs_output_path, 'wb')
            try:
                self.data_store_lock.acquire()
                read_only_data_store = TarFile(self.storage_file, 'r')
                self.data_store_lock.release()
                for c in meta_info.get_chunk_names():
                    self.data_store_lock.acquire()
                    untared_file = read_only_data_store.extractfile(c)
                    self.data_store_lock.release()
                    if untared_file == None:
                        logging.fatal("could not extract " + c +
                                      " from local store")
                    else:
                        output_file.write(untared_file.read())
            except:
                logging.exception("problem while writing " +
                                  dfs_path + " to " + fs_output_path)
            read_only_data_store.close()
            output_file.close()

    def ls_files(self):
        return self.mdm.ls_files()

    def ls_all_chunks(self):
        return self.mdm.ls_chunks()

    def ls_nodes(self):
        return self.mdm.ls_nodes()

    def started(self):
        return True

    def stop(self):
        self.pyro_daemon_loop_cond = False

def launch_local_meta_data_manager():
    Pyro.core.initServer()
    daemon = Pyro.core.Daemon(port = meta_data_manager_port)
    mdm = MetaDataManager()
    uri = daemon.connect(mdm, 'meta_data_manager') # publish object
    daemon.requestLoop(condition=lambda: mdm.pyro_daemon_loop_cond)
    # the following is executed only after mdm.stop() was called
    daemon.disconnect(mdm)
    daemon.shutdown()
    sys.exit(0)

def launch_local_data_manager():
    Pyro.core.initServer()
    daemon = Pyro.core.Daemon(port = data_manager_port)
    dm = DataManager()
    dm.put("/proc/cpuinfo","cpuinfo") # a test file for tests
    uri = daemon.connect(dm, 'data_manager') # publish object
    daemon.requestLoop(condition=lambda: dm.pyro_daemon_loop_cond)
    # the following is executed only after dm.stop() was called
    daemon.disconnect(dm)
    daemon.shutdown()
    sys.exit(0)

def usage():
    print """usage:
    command [parameters]
    ---
    app dfs_name   local_file   - append file to a local one
    cat dfs_name                - output file to screen
    get dfs_name   [local_file] - retrieve a file
    h[elp]                      - the present prose
    k[ill]                      - stop local data deamons then exit
                                  (DataManager and MetaDataManager)
    lmdm                        - use the local MetaDataManager (default)
    ls                          - list files
    lsac                        - list all chunks
    lslc                        - list local chunks only
    lsn                         - list nodes
    put local_file [dfs_name]   - publish a file
    q[uit] | e[xit]             - stop this wonderful program
    rmdm host [port]            - use a remote MetaDataManager
    """

if __name__ == '__main__':
    logging.basicConfig(level  = logging.DEBUG,
                        format = '%(asctime)s %(levelname)s %(message)s')
    dm_URI  = ("PYROLOC://localhost:" + str(data_manager_port) +
               "/data_manager")
    mdm_URI = ("PYROLOC://localhost:" + str(meta_data_manager_port) +
               "/meta_data_manager")
    dm  = Pyro.core.getProxyForURI(dm_URI)
    mdm = Pyro.core.getProxyForURI(mdm_URI)
    dm_already_here  = False
    mdm_already_here = False
    try:
        ignore = mdm.started()
        mdm_already_here = True
    except Pyro.errors.ProtocolError:
        # no local MetaDataManager running
        print("starting MetaDataManager daemon...")
        pid = os.fork()
        if pid == 0: # child process
            launch_local_meta_data_manager()
    if not mdm_already_here:
        time.sleep(0.1) # wait for him to enter his infinite loop
    else:
        print("MetaDataManager daemon OK")
    try:
        ignore = dm.started()
        dm_already_here = True
    except Pyro.errors.ProtocolError:
        # no local DataManager running
        print("starting DataManager daemon...")
        pid = os.fork()
        if pid == 0: # child process
            launch_local_data_manager()
    if not dm_already_here:
        time.sleep(0.1) # wait for him to enter his infinite loop
    else:
        print("DataManager daemon OK")
    try:
        usage()
        while True:
            sys.stdout.write("dfs# ") # a cool prompt
            read = sys.stdin.readline().strip()
            if len(read) == 0:
                usage()
            else:
                splitted = read.split()
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
                    print "connected to local MetaDataManager"
                elif command == "rmdm":
                    if argc == 2:
                        dm.use_remote_mdm(param_1)
                        print "connected to remote MetaDataManager"
                    elif argc == 3:
                        dm.use_remote_mdm(param_1, param_2)
                        print "connected to remote MetaDataManager"
                    else:
                        logging.error("need one or two params")
                        usage()
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
                        usage()
                    else:
                        dm.put(param_1, param_2)
                elif command == "get":
                    if argc not in [2, 3]:
                        logging.error("need one or two params")
                        usage()
                    else:
                        dm.get(param_1, param_2)
                elif command == "app":
                    if argc not in [3]:
                        logging.error("need two params")
                        usage()
                    else:
                        dm.get(param_1, param_2, True)
                elif command == "cat":
                    if argc not in [2]:
                        logging.error("need one param")
                        usage()
                    else:
                        dm.get(param_1, "/dev/stdout")
                elif command in ["q","quit", "e", "exit"]:
                    sys.exit(0)
                else:
                    logging.error("unknown command: " + command)
                    usage()
    except KeyboardInterrupt:
        pass
