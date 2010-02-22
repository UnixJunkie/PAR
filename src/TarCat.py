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
---

Catenate files inside a tar file.
"""

import sys

from tarfile import TarFile

class TarCat:

    def __init__(self, output_file):
        self.to_write = None
        try:
            # FBR: kind of dirty, file opened here
            #      but closed by the interpreter when object is GCed
            self.to_write = TarFile(output_file, 'w')
        except:
            print "exception: ", sys.exc_info()[0]

    def next(self, block_filename):
        try:
            self.to_write.add(block_filename)
        except:
            print "exception: ", sys.exc_info()[0]
