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
"""

import logging, socket, sys
import Pyro.core, Pyro.naming

from Chunk           import Chunk
from MetaDataManager import MetaDataManager
from Pyro.errors     import NamingError

class DataManager:

    def __init__(self, storage_file, remote_server, remote_port):
        # when compressing, we must compress before cuting into chunks so we
        # will have fewer chunks to transfer instead of having smaller ones
        # (better for network latency I think)
        self.CHUNK_SIZE = 1024*1024
        self.data_store = storage_file
        # FBR: create and write to it right now so we are sure it will work
        #      later on
        self.hostname   = socket.getfqdn()
        self.chunks     = {}
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
            logging.fatal("Couldn't find object, nameserver says: " + x)
            raise SystemExit
        self.mdm = Pyro.core.getProxyForURI(URI)

    def put(self, file):
        # FBR: finish this
        pass

    def get(self):
        pass

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
    #      - instantiate the MetaDataManager in parallel.py later
    #        and call its methods remotely
    mdm = MetaDataManager()
    commands      = ["ls", "put", "get", "quit", "q", "exit"]
    correct_argcs = [2,3,4]
    argc = len(sys.argv)
    if argc not in correct_argcs:
        usage()
    command = sys.argv[1]
    param_1 = None
    if argc == 3:
        param_1 = sys.argv[2]
    param_2 = None
    if argc == 4:
        param_2 = sys.argv[3]
    if command == "ls":
        if argc != 2:
            logging.error("ls takes no argument")
            usage()
        logging.debug("going to exec: " + command)
    elif command == "put":
        if argc not in [3,4]:
            logging.error("put takes one or two arguments")
            usage()
        logging.debug("going to exec: " + command)
        # code to use here soon
        # if os.path.isfile (self.name):
        #     try:
        #         self.size = os.path.getsize(self.name)
        #     except os.error:
        #         logging.exception ("can't get size of " + self.name)
        # else:
        #     logging.error ("no file " + self.name)
    elif command == "get":
        if argc not in [3,4]:
            logging.error("get takes one or two arguments")
            usage()
        logging.debug("going to exec: " + command)
    elif command in ["quit", "q", "exit"]:
        sys.exit(0)
    else:
        logging.error("unknown command: " + command)
        usage()
