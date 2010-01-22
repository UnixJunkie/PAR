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

import logging, thread

from StringIO import StringIO
from MetaData import MetaData

class MetaDataManager:
    def __init__(self):
        # FBR: maybe this logger config will move somewhere else
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(message)s')
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
