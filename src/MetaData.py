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

import time

class MetaData:
    # DataManager must not create this object, he must only give all info
    # to create it to the MetaDataManager which will create and then handle
    # this object
    def __init__(self, dfs_path, size, nb_chunks,
                 chunk_checksums = None):
        self.dfs_path      = dfs_path
        self.creation_time = time.time()
        self.size          = size
        self.nb_chunks     = nb_chunks
        self.chunks_list   = []
        if dfs_path.startswith('/'):
            dfs_path = dfs_path[1:]
        if chunk_checksums:
            for c in range(nb_chunks):
                self.chunks_list.append((str(c) + '/' + dfs_path,
                                         chunk_checksums[c]))
        else:
            for c in range(nb_chunks):
                self.chunks_list.append((str(c) + '/' + dfs_path,
                                         None))

    def get_uniq_ID(self):
        return self.dfs_path

    def get_chunk_name_and_sums(self):
        return self.chunks_list
