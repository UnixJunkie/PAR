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
from Chunk           import Chunk
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
        self.chunks         = {}
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
        return str(chunk_number) + "/" + dfs_path

    def chunk_name_to_index(self, chunk_name):
        return int((chunk_name.split("/"))[0])

    def add_local_chunk(self, chunk_number, dfs_path, tmp_file):
        self.lock.acquire()
        tar_info = (self.data_store.gettarinfo
                    (arcname = self.get_chunk_name(chunk_number, dfs_path),
                     fileobj = tmp_file))
        self.data_store.addfile(tar_info, tmp_file)
        self.data_store.fileobj.flush()
        self.lock.release()

    # publish a local file into the DFS
    def put(self, filename, dfs_path = None):
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
        meta_info = self.mdm.get_meta_data(dfs_path)
        all_chunks = meta_info.chunks
        non_local_chunks = []
        # shuffle should increase pipelining and parallelization of transfers
        for k in all_chunks.keys():
            source_hosts = all_chunks[k]
            # FBR: linear search instead of instantaneous lookup...
            if self.hostname not in source_hosts:
                non_local_chunks.append((k, random.shuffle(source_hosts)))
        random.shuffle(non_local_chunks)
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
            for i in range(meta_info.nb_chunks):
                c = self.get_chunk_name(i, dfs_path)
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
    DataManager.py command [parameters]
    ---
    available commands:
    ls                      : list all files
    put filename [dfs_path] : publish a file [under the identifier dfs_path]
                              (default dfs_path is filename)
    get dfs_path [filename] : retrieve a file and write it [to filename]
                              (default filename is dfs_path)
    {quit|q|exit}           : stop the program
    """
    sys.exit(0)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s')
    # FBR: - put this in an infinite loop
    #      - fork the DataManager thread out as a background daemon
    #      - find a way to communicate with him locally after he was forked
    print commands.getoutput("echo 0 `date`")
    dm = DataManager(commands.getoutput("hostname"), 9090)
    print commands.getoutput("echo 1 `date`")
    dm.put("/tmp/big_file")
    print commands.getoutput("echo 2 `date`")
    print(dm.mdm.ls())
    print commands.getoutput("echo 3 `date`")
    dm.get("/tmp/big_file", "/tmp/big_file_from_dfs")
    print commands.getoutput("echo 4 `date`")
#     commands      = ["ls", "put", "get", "quit", "q", "exit"]
#     correct_argcs = [2,3,4]
#     argc = len(sys.argv)
#     if argc not in correct_argcs:
#         usage()
#     command = sys.argv[1]
#     param_1 = None
#     if argc == 3:
#         param_1 = sys.argv[2]
#     param_2 = None
#     if argc == 4:
#         param_2 = sys.argv[3]
#     if command == "ls":
#         if argc != 2:
#             logging.error("ls takes no argument")
#             usage()
#         logging.debug("going to exec: " + command)
#     elif command == "put":
#         if argc not in [3,4]:
#             logging.error("put takes one or two arguments")
#             usage()
#         logging.debug("going to exec: " + command)
#     elif command == "get":
#         if argc not in [3,4]:
#             logging.error("get takes one or two arguments")
#             usage()
#         logging.debug("going to exec: " + command)
#     elif command in ["quit", "q", "exit"]:
#         sys.exit(0)
#     else:
#         logging.error("unknown command: " + command)
#         usage()
