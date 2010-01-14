#!/usr/bin/env python2.6

import time
import operator
from stream import ThreadedFeeder, ForkedFeeder, map, reduce

"""
Demonstrate the use of a feeder to minimize time spent by the whole pipeline
waiting for a blocking producer.

$ time python ./feeder.py     # use processes

real    0m7.186s
user    0m7.026s
sys     0m0.033s

$ time python ./feeder.py -t   # use threads

real    0m7.231s
user    0m7.046s
sys     0m0.020s

$ time python ./feeder.py -s  # sequential

real    0m13.072s
user    0m7.596s
sys     0m0.067s
"""

def blocking_producer():
	for n in range(25):
		time.sleep(0.01)
		yield 42

if __name__ == '__main__':
	f = lambda x: x**x**3
	import sys
	try:
		if sys.argv[1] == '-s':
			## use a single thread
			blocking_producer() >> map(f) >> reduce(operator.add)
		elif sys.argv[1] == '-t':
			## use a feeder in a separate thread
			ThreadedFeeder(blocking_producer) >> map(f) >> reduce(operator.add)
	except IndexError:
		## use a feeder in a child process
		ForkedFeeder(blocking_producer) >> map(f) >> reduce(operator.add)
