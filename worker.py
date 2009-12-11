#!/usr/bin/python

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

import Pyro.naming, Pyro.core

from Pyro.errors import NamingError

print 'Locating Name Server...'
locator = Pyro.naming.NameServerLocator()
ns = locator.getNS()
print 'Locating Master...'
try:
    URI = ns.resolve('master')
    print 'URI:',URI
except NamingError,x:
    print "Couldn't find object, nameserver says:",x
    raise SystemExit

master = Pyro.core.getProxyForURI(URI)

work = master.get_work()
while work != "":
    res = work + " done!"
    master.put_result(res)
    work = master.get_work()
