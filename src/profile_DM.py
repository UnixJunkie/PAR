#!/usr/bin/env python

import profile

from DataManager import DataManager

def uput_prof(dm):
    dm.put("/tmp/db.tar", verify = False)

def get_prof(dm):
    dm.get("/tmp/db.tar", "/tmp/out")

# dm = DataManager(profiling = True)
# profile.run('uput_prof(dm)')

dm = DataManager(profiling = True)
dm.put("/tmp/db.tar", verify = False)
profile.run('get_prof(dm)')
