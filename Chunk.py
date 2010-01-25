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

import logging

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
