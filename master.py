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

import Pyro.core
import Pyro.naming
import thread

from Pyro.errors import PyroError, NamingError
from Queue       import Queue, Empty

class Master(Pyro.core.ObjBase):
    def __init__(self):
        Pyro.core.ObjBase.__init__(self)
        self.jobs_queue     = Queue()
        self.results_queue  = Queue()
        self.finished       = False
        self.lock           = thread.allocate_lock()
        self.jobs_queue.put("echo toto")
        self.jobs_queue.put("echo titi")
        self.jobs_queue.put("echo tata")
        self.nb_jobs        = self.jobs_queue.qsize()

    def get_work(self):
        res = ""
        try:
            res = self.jobs_queue.get(True, 1)
        except Empty:
            must_print = False
            self.lock.acquire() # -- critic section
            if self.results_queue.qsize() == self.nb_jobs:
                self.finished = True
                must_print    = True
            self.lock.release() # -- /critic section
            if must_print:
                for _ in range(self.nb_jobs):
                    print self.results_queue.get()
        return res

    def put_result(self, res):
        self.results_queue.put(res)

if __name__ == "__main__":
        Pyro.core.initServer()
        daemon = Pyro.core.Daemon()
        print 'Locating Name Server...'
        locator = Pyro.naming.NameServerLocator()
        ns = locator.getNS()
        daemon.useNameServer(ns)
        # connect a new object (unregister previous one first)
        try:
            # 'master' is our outside world name
            ns.unregister('master')
        except NamingError:
            pass
        # publish master object
        master = Master()
        daemon.connect(master,'master')
        try:
            # start infinite loop
            print 'Master started'
            daemon.requestLoop()
        except KeyboardInterrupt:
            daemon.disconnect(master)
            daemon.shutdown()
