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
    def __init__(self):
        Pyro.core.ObjBase.__init__(self)
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
        self.nodes  = [] # chunk sources

    def ls_files(self):
        res = []
        self.files_lock.acquire()
        for v in self.files.values():
            res.append(v.get_uniq_ID())
        self.files_lock.release()
        return res

    def ls_chunks(self):
        res = []
        self.chunks_lock.acquire()
        for v in self.chunks.keys():
            res.append(v + ':' + str(self.chunks[v]))
        self.chunks_lock.release()
        return res

    def ls_nodes(self):
        self.chunks_lock.acquire()
        res = list(self.nodes)
        self.chunks_lock.release()
        return res

    def resolve(self, chunks_list):
        res = []
        self.chunks_lock.acquire()
        for c in chunks_list:
            res.append((c, self.chunks[c]))
        self.chunks_lock.release()
        return res

    # publish a new file's meta data object
    def publish_meta_data(self, dfs_path, publication_host, size, nb_chunks):
        give_up = False
        to_publish = MetaData(dfs_path, publication_host, size, nb_chunks)
        uid = to_publish.get_uniq_ID()
        self.files_lock.acquire()
        if self.files.get(uid) == None:
            self.files[uid] = to_publish
        else:
            logging.error("can't overwrite: " + uid)
            give_up = True
        self.files_lock.release()
        if not give_up:
            if dfs_path.startswith('/'):
                dfs_path = dfs_path[1:]
            self.chunks_lock.acquire()
            self.nodes.append(publication_host)
            for c in to_publish.get_chunk_names():
                self.chunks[c] = [publication_host]
            self.chunks_lock.release()

    # retrieve a file's meta data
    def get_meta_data(self, dfs_path):
        self.files_lock.acquire()
        res = self.files.get(dfs_path)
        self.files_lock.release()
        return res

    # augment nodes list for an existing file chunk
    def update_add_node(self, dfs_path, chunk_ID, node_name):
        self.files_lock.acquire()
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
        self.files_lock.release()

    # shrink nodes list for an existing file chunk
    def update_remove_node(self, dfs_path, chunk_ID, node_name):
        pass
