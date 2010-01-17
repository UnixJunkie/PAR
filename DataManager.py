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

# What are the external commands users will call on a DataManager?
##################################################################
# * the API must closely reflect these commands to facilitate
#   implementation
# * the DataManager will contact the MetadataManager to get info it
#   does not know
# - list                      : list all files
# - put filename [dfs_path]   : publish it [under the identifier dfs_path]
# - get dfs_path [local_path] : retrieve a file and write it [to local_path]

