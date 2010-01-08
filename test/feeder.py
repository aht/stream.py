#!/usr/bin/env python2.6

import time
import operator
import os, sys

from pprint import pprint

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from stream import ThreadedFeeder, ForkedFeeder, map, reduce


## Test script based on ../example/feeder.py

def blocking_producer():
	n = 0
	while 1:
		time.sleep(0.01)
		n += 1
		if n < 25:
			yield 42
		else:
			raise StopIteration

f = lambda x: x**2

def test_feeder():
	a = blocking_producer() >> map(f) >> reduce(operator.add)
	b = ThreadedFeeder(blocking_producer) >> map(f) >> reduce(operator.add)
	c = ForkedFeeder(blocking_producer) >> map(f) >> reduce(operator.add)
	pprint(a)
	pprint(b)
	pprint(c)
	assert a == b
	assert a == c
	assert b == c
	

if __name__ == '__main__':
	import nose
	nose.main()
