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

import commands, logging, os, random, socket, sys, stat, thread
# import Pyro.core, Pyro.naming

from tarfile         import TarFile
from tempfile        import TemporaryFile
from MetaDataManager import MetaDataManager
# from Pyro.errors     import NamingError

class DataManager:

    def __init__(self, remote_server, remote_port):
        # when compressing, we must compress before cuting into chunks so we
        # will have fewer chunks to transfer instead of having smaller ones
        # (better for network latency I think)
        ##################################
        # SYNCHRONIZE ACCES TO DATASTORE #
        ##################################
        self.lock           = thread.allocate_lock()
        self.CHUNK_SIZE     = 1024*1024
        self.data_store     = None
        self.hostname       = socket.getfqdn()
        self.storage_file   = ("/tmp/dfs_" + os.getlogin() +
                               "_at_" + self.hostname)
        self.local_chunks   = {}
        try:
            # The TarFile interface forces us to use a temporary file.
            # Maybe it makes some unnecessary data copy, however having
            # our chunks kept in the standard tar format is too cool
            # to be changed
            self.lock.acquire()
            self.data_store = TarFile(self.storage_file, 'w')
            os.chmod(self.data_store.fileobj.name,
                     stat.S_IRUSR | stat.S_IWUSR) # <=> chmod 600 ...
            self.lock.release()
            temp_file = TemporaryFile()
            temp_file.write("DFS_STORAGE_v00\n")
            temp_file.flush()
            temp_file.seek(0)
            self.add_local_chunk(0, "STORAGE_INITIALIZED", temp_file)
            temp_file.close()
        except:
            logging.exception("can't create or write to: " + self.storage_file)
#         logging.info('Locating Name Server...')
#         locator = Pyro.naming.NameServerLocator()
#         ns = locator.getNS(host = remote_server,
#                            port = remote_port)
#         logging.info('Located')
#         try:
#             logging.info('Locating meta_data_manager...')
#             URI = ns.resolve('meta_data_manager')
#             logging.info('Located')
#         except NamingError,x:
#             logging.exception("Couldn't find object, nameserver says: " + x)
#             raise SystemExit
#        self.mdm = Pyro.core.getProxyForURI(URI)
        self.mdm = MetaDataManager()

    def get_chunk_name(self, chunk_number, dfs_path):
        if dfs_path.startswith('/'):
            dfs_path = dfs_path[1:]
        return str(chunk_number) + '/' + dfs_path

    def chunk_name_to_index(self, chunk_name):
        return int((chunk_name.split('/'))[0])

    def add_local_chunk(self, chunk_number, dfs_path, tmp_file):
        chunk_name = self.get_chunk_name(chunk_number, dfs_path)
        self.lock.acquire()
        tar_info = self.data_store.gettarinfo (arcname = chunk_name,
                                               fileobj = tmp_file)
        self.data_store.addfile(tar_info, tmp_file)
        self.data_store.fileobj.flush()
        self.local_chunks[chunk_name] = True
        self.lock.release()

    def ls_local_chunks(self):
        res = []
        self.lock.acquire()
        res = self.local_chunks.keys()
        self.lock.release()
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
    def get(self, dfs_path, fs_output_path):
        if fs_output_path == None:
            fs_output_path = dfs_path
        # shuffle is here to increase pipelining and parallelization
        # of transfers
        remote_chunks = []
        meta_info = self.mdm.get_meta_data(dfs_path)
        if meta_info == None:
            logging.error("no such file: " + dfs_path)            
        else:
            self.lock.acquire()
            for c in meta_info.get_chunk_names():
                if self.local_chunks.get(c) == None:
                    remote_chunks.append(c)
            self.lock.release()
            random.shuffle(remote_chunks)
            remote_chunks_mapping = self.mdm.resolve(remote_chunks)
            # FBR: TODO download them then publish current host as hosting
            #      this chunks too
            #
            # dump all chunks from local store to fs_output_path and
            # in the right order please
            original_dfs_path = dfs_path
            output_file = open(fs_output_path, 'wb')
            try:
                self.lock.acquire()
                read_only_data_store = TarFile(self.storage_file, 'r')
                self.lock.release()
                if dfs_path.startswith('/'):
                    dfs_path = dfs_path[1:]
                for c in meta_info.get_chunk_names():
                    self.lock.acquire()
                    untared_file = read_only_data_store.extractfile(c)
                    self.lock.release()
                    if untared_file == None:
                        logging.fatal("could not extract " + c +
                                      " from local store")
                    else:
                        output_file.write(untared_file.read())
            except:
                logging.exception("problem while writing " + original_dfs_path +
                                  " to " + fs_output_path)
            read_only_data_store.close()
            output_file.close()

# What are the external commands users will call on a DataManager?
##################################################################
# * the API must closely reflect these commands to facilitate
#   implementation
# * the DataManager will contact the MetadataManager to get info it
#   does not know
def usage():
    #0              1    1   2        3            1   2        3
    print """usage:
    command [parameters]
    ---
    ls                          - list files
    lsc                         - list chunks
    lsn                         - list nodes
    put local_file [dfs_name]   - publish a file
    get dfs_name   [local_file] - retrieve a file
    cat dfs_name                - output file to screen
    q[uit] | e[xit]             - stop this wonderful program
    h[elp]                      - the present prose
    """

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s')
    # FBR: - fork this as a daemon reading from a named pipe and writing
    #        to a log file, prevent creation if there is already a
    #        local store tar file
    #      - add a thread to manage data trsnafer
    dm = DataManager(commands.getoutput("hostname"), 9090)
    dm.put("/proc/cpuinfo") # to have a test file in dfs for CLI tests
    commands = ["ls", "put", "get", "help", "h", "quit", "q", "exit", "e"]
    try:
        usage()
        while True:
            sys.stdout.write("dfs# ") # what a cool prompt!!! :)
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
                elif command == "ls":
                    print "files:"
                    print dm.mdm.ls_files()
                elif command == "lsc":
                    print "chunks:"
                    print dm.mdm.ls_chunks()
                elif command == "lsn":
                    print "nodes:"
                    print dm.mdm.ls_nodes()
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
                elif command == "cat":
                    if argc not in [2]:
                        logging.error("need one param")
                        usage()
                    else:
                        dm.get(param_1, "/dev/stdout")
                elif command in ["quit", "q", "exit", "e"]:
                    sys.exit(0)
                else:
                    logging.error("unknown command: " + command)
                    usage()
    except KeyboardInterrupt:
        pass
