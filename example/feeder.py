import time
import operator
from stream import ThreadedFeeder, ForkedFeeder, map, reduce

"""
Demonstrate the use of a feeder to minimize time spent by the whole pipeline
waiting for a blocking producer.
"""

def blocking_producer():
	n = 0
	while 1:
		time.sleep(0.05)
		n += 1
		if n < 100:
			yield 42
		else:
			raise StopIteration

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
