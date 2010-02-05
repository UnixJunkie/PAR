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

import logging, thread
import Pyro.core, Pyro.naming

from MetaData import MetaData

class MetaDataManager(Pyro.core.ObjBase):

    def __init__(self, debug = False):
        Pyro.core.ObjBase.__init__(self)
        self.debug = debug
        # FBR: maybe this logger config will move somewhere else
        logging.basicConfig(level  = logging.DEBUG,
                            format = '%(asctime)s %(levelname)s %(message)s')
        self.files_lock  = thread.allocate_lock()
        self.chunks_lock = thread.allocate_lock()
        ################################################
        # PREVENT CONCURRENT ACCES TO THESE ATTRIBUTES #
        ################################################
        self.files  = {} # MetaData objects indexed by their dfs_path
        self.chunks = {} # mapping chunk_ID -> source nodes list
        self.nodes  = {} # chunk sources
        self.pyro_daemon_loop_cond = True

    def ls_files(self):
        self.files_lock.acquire()
        if self.debug: print "self.files_lock ACK"
        values = list(self.files.values())
        self.files_lock.release()
        if self.debug: print "self.files_lock REL"
        res = []
        for v in values:
            res.append(v.get_uniq_ID())
        return res

    def ls_chunks(self):
        self.chunks_lock.acquire()
        if self.debug: print "self.chunks_lock ACK"
        keys = list(self.chunks.keys())
        self.chunks_lock.release()
        if self.debug: print "self.chunks_lock REL"
        res = []
        for v in keys:
            res.append(v + ':' + str(self.chunks[v]))
        return res

    def ls_nodes(self):
        self.chunks_lock.acquire()
        if self.debug: print "self.chunks_lock ACK"
        res = list(self.nodes.keys())
        self.chunks_lock.release()
        if self.debug: print "self.chunks_lock REL"
        return res

    def resolve(self, chunk_name):
        res = []
        self.chunks_lock.acquire()
        if self.debug: print "self.chunks_lock ACK"
        res = self.chunks[chunk_name]
        self.chunks_lock.release()
        if self.debug: print "self.chunks_lock REL"
        return res

    # publish a new file's meta data object
    def publish_meta_data(self, dfs_path, publication_host, size, nb_chunks,
                          chunk_checksums = None):
        give_up = False
        to_publish = MetaData(dfs_path, size, nb_chunks, chunk_checksums)
        uid = to_publish.get_uniq_ID()
        self.files_lock.acquire()
        if self.debug: print "self.files_lock ACK"
        if self.files.get(uid) == None:
            self.files[uid] = to_publish
        else:
            logging.error("can't overwrite: " + uid)
            give_up = True
        self.files_lock.release()
        if self.debug: print "self.files_lock REL"
        if not give_up:
            if dfs_path.startswith('/'):
                dfs_path = dfs_path[1:]
            self.chunks_lock.acquire()
            if self.debug: print "self.chunks_lock ACK"
            if self.nodes.get(publication_host) == None:
                self.nodes[publication_host] = True
            for (c, _) in to_publish.get_chunk_name_and_sums():
                self.chunks[c] = [publication_host]
            self.chunks_lock.release()
            if self.debug: print "self.chunks_lock REL"

    # retrieve a file's meta data
    def get_meta_data(self, dfs_path):
        self.files_lock.acquire()
        if self.debug: print "self.files_lock ACK"
        res = self.files.get(dfs_path)
        self.files_lock.release()
        if self.debug: print "self.files_lock REL"
        return res

    # augment nodes list for an existing file chunk
    def update_add_node(self, dfs_path, chunk_ID, publication_host):
        if dfs_path.startswith('/'):
            dfs_path = dfs_path[1:]
        c = str(chunk_ID) + "/" + dfs_path
        self.chunks_lock.acquire()
        if self.debug: print "self.chunks_lock ACK"
        self.nodes[publication_host] = True
        self.chunks[c].append(publication_host)
        self.chunks_lock.release()
        if self.debug: print "self.chunks_lock REL"

    # shrink nodes list for an existing file chunk
    def update_remove_node(self, dfs_path, chunk_ID, node_name):
        pass

    def started(self):
        return True

    def stop(self):
        self.pyro_daemon_loop_cond = False
