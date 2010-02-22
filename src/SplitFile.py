#!/usr/bin/env python

"""
If you use and like our software, please send us a postcard! ^^

Copyright (C) 2009, 2010, Zhang Initiative Research Unit,
Advance Science Institute, Riken
2-1 Hirosawa, Wako, Saitama 351-0198, Japan
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
"""

import math, os, sys

class SplitFile:

    def __init__(self, input_file, nb_splits):
        self.nb_splits  = float(nb_splits)
        self.index      = 0
        self.abspath    = ""
        self.to_read    = None
        self.block_size = 0
        try:
            self.abspath    = os.path.abspath(input_file)
            self.block_size = int(math.ceil(
                float(os.path.getsize(input_file)) / self.nb_splits))
            self.to_read    = open(self.abspath, 'rb')
        except:
            print "exception: ", sys.exc_info()[0]

    def next(self):
        res  = str(self.index) + '_' + self.abspath.replace(os.sep, '_')
        buff = self.to_read.read(self.block_size)
        if buff == '':
            raise StopIteration
        try:
            to_write = open(res, 'wb')
            to_write.write(buff)
            to_write.close()
        except:
            print "exception: ", sys.exc_info()[0]
        self.index += 1
        return res

    def __iter__(self):
        return self
