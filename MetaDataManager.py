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

# What is a file?
# metadata:
# =========
# - a name
# - a size
# - a creation time and date
#   [optionaly]
#   - a user
#   - a group
#   - permissions for UGO <-- NOTE: can't we use ACL grain permissions?
# data:
# =====
# - a list of chunks

# What is a chunk?
# - some data
# - the list of nodes where this data is stored, on each node there is
#   a path corresponding to where we can read the data on the filesystem
#   NOTE: maybe having each chunk in a distinct file is not a good idea,
#         maybe a tar holding all the chunks of the machine, or a Berkeley DB,
#         or a flat file we only append to is better. But beware of a possible
#         bottleneck also (several write at the same time on the same node
#         but from different threads).

