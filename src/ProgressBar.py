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
import time

# seconds to (hour, min, second)
def seconds_to_h_m_s(dt_s):
  h = int(dt_s / 3600)
  m = int((dt_s - (h * 3600)) / 60)
  s = dt_s - (h * 3600) - (m * 60)
  return (h, m, s)

class ProgressBar:

  def __init__(self, min_val, max_val):
    self.min      = float(min_val)
    self.max      = float(max_val)
    self.width    = self.max - self.min
    self.done     = 0
    self.current  = "done: %3d " % self.done + "%"
    self.previous = None
    self.start    = time.time()
    self.update(0)

  def update(self, new_amount):
    if self.width > 0.0:
      now          = time.time()
      dt_s         = now - self.start
      new_amount   = float(new_amount)
      new_amount   = max(new_amount, self.min)
      new_amount   = min(new_amount, self.max)
      self.done    = new_amount
      delta        = self.done - self.min
      done_frac    = float(delta / self.width)
      percent      = int(round(done_frac * 100.0))
      if done_frac > 0:
        # estimated_total - already_elapsed
        eta_s = (dt_s / done_frac) - dt_s
        h, m, s = seconds_to_h_m_s(eta_s)
        self.current = "done: %3d%% ETA: %dh%02dmin%02ds" % (percent, h, m, s)
      else:
        self.current = "done:   0% ETA: ???"

  def draw(self):
    if self.current != self.previous:
      self.previous = self.current
      sys.stdout.write(self.current + '\r')
      if self.done == self.max:
        sys.stdout.write('\n') # prevent overwriting
      sys.stdout.flush()
