#!/usr/bin/env python2.4

import profile

from DataManager import DataManager

def uput_prof(dm):
    dm.put("/tmp/par/512", verify = False)

def get_prof(dm):
    dm.get("/tmp/par/512", "/tmp/out")

# FBR: test 1
# for i in range(10):
#     dm = DataManager()
#     uput_prof(dm)

# dm = DataManager()
# profile.run('uput_prof(dm)')

# FBR: test 2
# dm = DataManager()
# dm.put("/tmp/par/512", verify = False)
# for i in range(10):
#     get_prof(dm)

dm = DataManager()
dm.put("/tmp/par/512", verify = False)
profile.run('get_prof(dm)')
