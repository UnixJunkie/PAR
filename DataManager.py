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

import commands, logging, random, socket, sys
import Pyro.core, Pyro.naming

from tarfile         import TarFile
from Chunk           import Chunk
from MetaDataManager import MetaDataManager
from Pyro.errors     import NamingError

class DataManager:

    def __init__(self, storage_file, remote_server, remote_port):
        # when compressing, we must compress before cuting into chunks so we
        # will have fewer chunks to transfer instead of having smaller ones
        # (better for network latency I think)
        self.CHUNK_SIZE     = 1024*1024
        self.data_store     = None
        self.hostname       = socket.getfqdn()
        self.chunks         = {}
        self.temp_file      = None
        self.temp_file_name = commands.getoutput("echo /tmp/$USER.hostname")
        try:
            # FBR: we should use an indexed file rather than tar to avoid
            #      using a temporary file
            #      The index should contain begin and end offset of each chunk
            self.data_store = TarFile(storage_file, 'w')
            try:
                self.temp_file = open(self.temp_file_name, 'w')
                self.temp_file.write("DFS_STORAGE_v00\n")
                self.temp_file.flush()
                self.add_local_chunk(0, self.temp_file_name)
            except:
                logging.exception("can't create or write to: " +
                                  self.temp_file_name)
        except:
            logging.exception("can't create or write to: " + storage_file)
        logging.info('Locating Name Server...')
        locator = Pyro.naming.NameServerLocator()
        ns = locator.getNS(host = remote_server,
                           port = remote_port)
        logging.info('Located')
        try:
            logging.info('Locating meta_data_manager...')
            URI = ns.resolve('meta_data_manager')
            logging.info('Located')
        except NamingError,x:
            logging.exception("Couldn't find object, nameserver says: " + x)
            raise SystemExit
        self.mdm = Pyro.core.getProxyForURI(URI)

    def create_chunk_name(self, chunk_number, dfs_path):
        return str(chunk_number) + "/" + dfs_path

    def chunk_name_to_index(self, chunk_name):
        return int((chunk_name.split("/"))[0])

    def add_local_chunk(self, chunk_number, dfs_path):
        self.data_store.add(self.temp_file.name,
                            self.create_chunk_name(chunk_number, dfs_path))

    # publish a local file into the DFS
    def put(self, filename, dfs_path = None):
        if dfs_path == None:
            dfs_path = filename
        # FBR: add compression of added file here
        try:
            file_size = 0
            chunk_number = 0
            input_file = open(filename, 'r')
            read_buff = input_file.read(self.CHUNK_SIZE)
            while read_buff != '':
                file_size += len(read_buff)
                self.temp_file.truncate(0)
                self.temp_file.write(read_buff)
                self.temp_file.flush()
                self.add_local_chunk(chunk_number, dfs_path)
                chunk_number += 1
                read_buff = input_file.read(self.CHUNK_SIZE)
            input_file.close()
            self.mdm.publish_meta_data(dfs_path, self.hostname,
                                       chunk_number, file_size)
        except:
            logging.exception("problem while reading " + filename)

    # download a DFS file and dump it to a local file
    def get(self, dfs_path, fs_output_path):
        # get file info
        meta_info = self.mdm.get_meta_data(dfs_path)
        # find list of chunks we need to retrieve
        all_chunks = meta_info.chunks
        non_local_chunks = []
        # I use shuffle to increase pipelining and parallelization
        # of chunk transfers
        for k in all_chunks.keys:
            source_hosts = all_chunks[k]
            if self.hostname not in source_hosts:
                non_local_chunks.append((k, random.shuffle(source_hosts)))
        random.shuffle(non_local_chunks)
        # download them FBR: TODO
        # dump all chunk from local store in the right order
        try:
            f = open(fs_output_path, 'w')
            for i in range(meta_info.nb_chunks):
                c = self.create_chunk_name(i, dfs_path)
                tar = self.data_store.extractfile(c)
                if tar == None:
                    logging.error("cound not extract " + c +
                                  " from local store")
                else:
                    f.write(tar.read)
            f.close()
        except:
            logging.exception("problem while writing to " + fs_output_path)

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
    #      - fork the DataManager thread out
    #      - find a way to communicate with him locally after he was forked
    dm = DataManager("/tmp/storage.tar", commands.getoutput("hostname"), 9090)
    dm.put("/tmp/big_file")
    print(dm.mdm.ls())
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
