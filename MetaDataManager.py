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

import logging, thread, time

from StringIO import StringIO

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')
# log levels:
#  debug
#  info
#  warning
#  error
#  critical
#  exception

# criteria to cut a file into chunks
# when compressing: we should compress before cuting into chunks so we will
#                   have fewer chunks to transfer instead of having smaller
#                   ones (better for latency I think)
CHUNK_SIZE = 1024*1024

# What is a chunk of Data?
##########################
# - chunks must be sortable (in order to retrieve how to recreate the file
#                            they belong to)
# - some data (possibly compressed)
# - data size
# - the list of nodes where this data is stored
#   on each node there is a path corresponding to the storage file
#   the storage backend is a GNU tar file containing only compressed (gzip -1)
#   files.
#   We'll think again later if we observe a bottleneck when writing massively
#   to it. Maybe then we will decide we need one backend per CPU to
#   parallelize writes on the same host (I/O load balancing).
class Chunk:
    def __init__(self, identifier, size, index, node, compressed = False):
        self.id            = identifier
        self.size          = size
        self.index         = index
        self.nodes         = [node]
        self.is_compressed = compressed

    # to call when one more node possess this chunk
    def add_node(self, node_name):
        self.nodes.append(node_name)

    # to call when one less node possess this chunk
    def remove_node(self, node_name):
        try:
            self.nodes.remove(node_name)
        except:
            logging.error("no node: " + node_name +
                          " for chunk: " + self.id)

# How is described a file?
##########################
# metadata: <-- managed by MetaDataManager (only one per parallel.py server)
# =========
# - a name
# - a size (of the uncompressed file)
# - a creation time and date
#   [optionaly]
#   - a user
#   - a group
#   - permissions for UGO <-- NOTE: ACLs are fare better than UGO
# data: <-- managed by DataManager (one per parallel.py client,
#                                   ??? plus one for the server ???)
#                                   the server could be run on a cluster
#                                   frontend, some clusters don't like
#                                   that nodes use the frontend's
#                                   disk/network/CPU too heavily
# maybe we should have DataManagers only inside the cluster, and maybe
# we need a special mode (--mirror), which means this special DataManager
# must have all data listed by the MetaDataManager
# =====
# - a dictionary of chunks
class MetaData:
    # DataManager must not create this object, he must only give all info
    # to create it to the MetaDataManager which will create and then handle
    # this object
    def __init__(self, size, filename, dfs_path = None):
        self.size          = size
        self.name          = filename
        self.dfs_path      = filename
        self.creation_time = time.time()
        self.chunks = {} # FBR: chunks list must be initialized
        # create its chunks ###
        if dfs_path != None:
            self.dfs_path = dfs_path

    def get_uniq_ID(self):
        return self.dfs_path

class MetaDataManager:
    def __init__(self):
        ############################################
        # PREVENT SYNCHRONIZED ACCES TO ATTRIBUTES #
        ############################################
        self.lock  = thread.allocate_lock()
        self.files = {} # must contain MetaData objects, indexed by their
                        # dfs_path

    # list files
    def ls(self):
        try:
            self.lock.acquire()
            values = self.files.values()
        finally:
            self.lock.release()
        # FBR: maybe we'll need to list things more extensively
        result = StringIO()
        for k in values:
            result.write(k + "\n")
        result.close()
        return result.getvalue()

    # publish a new file's meta data object
    def publish_meta_data(self, size, filename, dfs_path = None):
        to_publish = MetaData(size, filename, dfs_path)
        uid = to_publish.get_uniq_ID()
        try:
            self.lock.acquire()
            if self.files.get(uid) == None:
                self.files[uid] = to_publish
            else:
                logging.error("can't overwrite: " + uid)
        finally:
            self.lock.release()

    # augment nodes list for an existing file chunk
    def update_add_node(self, dfs_path, chunk_ID, node_name):
        try:
            self.lock.acquire()
            metadata = self.files.get(dfs_path)
            if metadata == None:
                logging.error("no file: " + dfs_path +
                              " can't add node for chunk: " + chunk_ID)
            else:
                chunk = metadata.get_chunk(chunk_ID)
                if chunk == None:
                    logging.error("no chunk: " + chunk_ID +
                                  " for file: " + dfs_path)
                else:
                    chunk.add_node(node_name)
        finally:
            self.lock.release()

    # shrink nodes list for an existing file chunk
    def update_remove_node(self, dfs_path, chunk_ID, node_name):
        try:
            self.lock.acquire()
            metadata = self.files.get(dfs_path)
            if metadata == None:
                logging.error("no file: " + dfs_path +
                              " can't del node for chunk: " + chunk_ID)
            else:
                chunk = metadata.get_chunk(chunk_ID)
                if chunk == None:
                    logging.error("no chunk: " + chunk_ID +
                                  " for file: " + dfs_path)
                else:
                    chunk.remove_node(node_name)
        finally:
            self.lock.release()
