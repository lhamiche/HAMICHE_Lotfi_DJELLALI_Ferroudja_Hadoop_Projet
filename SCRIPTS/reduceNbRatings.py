#!/usr/bin/env python
import sys

num_ratings = {}
for line in sys.stdin:

        movie, rating = line.split('\t')


        if  rating in num_ratings:
			num_ratings[rating] = num_ratings[rating]+count
		elif rating not in num_ratings:
			num_ratings[rating] = count

    for key in num_ratings:
		print "{0}\t{1}".format(key, num_ratings[key])
