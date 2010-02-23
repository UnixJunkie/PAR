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

import sys

class ProgressBar:
    
  def __init__(self, min_val, max_val):
    self.min      = float(min_val)
    self.max      = float(max_val)
    self.width    = self.max - self.min
    self.done     = 0
    self.current  = "done: %3d " % self.done + "%"
    self.previous = None
    self.update(0)

  def update(self, new_amount):
    if self.width > 0.0:
      new_amount   = float(new_amount)
      new_amount   = max(new_amount, self.min)
      new_amount   = min(new_amount, self.max)      
      self.done    = new_amount
      delta        = self.done - self.min
      percent      = int(round((delta / self.width) * 100.0))
      self.current = "done: %3d " % percent + "%"

  def draw(self):
    if self.current != self.previous:
      self.previous = self.current
      sys.stdout.write(self.current + '\r')
      if self.done == self.max:
        sys.stdout.write('\n') # prevent overwriting
      sys.stdout.flush()
