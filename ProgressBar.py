# obtained from http://code.activestate.com/recipes/168639/
# Copyright 2002, Randy Pargman
# Python license version 2.6.2
# You should have received a copy of the Python license along with this
# program.  If not, see:
# http://www.python.org/download/releases/2.6.2/license

import sys

class ProgressBar:
    def __init__(self, minValue = 0, maxValue = 10, totalWidth=12):
        self.progBar = "[]"   # This holds the progress bar string
        self.oldProgBar = self.progBar
        self.min = minValue
        self.max = maxValue
        self.span = maxValue - minValue
        self.width = totalWidth
        self.amount = 0       # When amount == max, we are 100% done
        self.updateAmount(0)  # Build progress bar string

    def updateAmount(self, newAmount = 0):
        if newAmount < self.min: newAmount = self.min
        if newAmount > self.max: newAmount = self.max
        self.amount = newAmount

        # Figure out the new percent done, round to an integer
        diffFromMin = float(self.amount - self.min)
        percentDone = (diffFromMin / float(self.span)) * 100.0
        percentDone = round(percentDone)
        percentDone = int(percentDone)

        # Figure out how many hash bars the percentage should be
        allFull = self.width - 2
        numHashes = (percentDone / 100.0) * allFull
        numHashes = int(round(numHashes))

        # build a progress bar with hashes and spaces
        self.progBar = "[" + '#'*numHashes + ' '*(allFull-numHashes) + "]"

        # figure out where to put the percentage, roughly centered
        percentPlace = (len(self.progBar) / 2) - len(str(percentDone))
        percentString = str(percentDone) + "%"

        # slice the percentage into the bar
        self.progBar = (self.progBar[0:percentPlace] + percentString +
                        self.progBar[percentPlace+len(percentString):])

    # draw progress bar only if it has changed
    def draw(self):
        if self.progBar != self.oldProgBar:
            self.oldProgBar = self.progBar
            sys.stdout.write(self.progBar + '\r')
            if self.amount == self.max:
                sys.stdout.write('\n')
            sys.stdout.flush()
