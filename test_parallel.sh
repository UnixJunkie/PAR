#!/bin/bash

# Copyright (C) 2009, Zhang Initiative Research Unit,
# Advance Science Institute, Riken
# 2-1 Hirosawa, Wako, Saitama 351-0198, Japan
# If you use and like our software, please send us a postcard! ^^
# ---
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
# ---

set -x

rm -f test_parallel.output

cat test_parallel.input | ./parallel.py -i /dev/stdin | cut -d':' -f2 |\
sed "s/^res(//g" | sed "s/)$//g" | grep -v "no more jobs for me" | sort -n > \
test_parallel.output

diff test_parallel.output test_parallel.output.reference
