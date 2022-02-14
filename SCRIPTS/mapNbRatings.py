#!/usr/bin/env python

import sys

for line in sys.stdin:
    userID, movieID, rating, timestamp = line.split('::')

    results = [movieID, rating]
print("\t".join(results))
