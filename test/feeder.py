#!/usr/bin/env python2.6

import time
import operator
import os, sys

from pprint import pprint

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from stream import ThreadedFeeder, ForkedFeeder, map, reduce


## Test scenario based on ../example/feeder.py

def blocking_producer():
	for n in range(25):
		time.sleep(0.01)
		yield 42

f = lambda x: x**2

expected = blocking_producer() >> map(f) >> reduce(operator.add)


## Test cases

def test_ThreadedFeeder():
	result = ThreadedFeeder(blocking_producer) >> map(f) >> reduce(operator.add)
	pprint(result)
	assert result == expected

def test_ForkedFeeder():
	result = ForkedFeeder(blocking_producer) >> map(f) >> reduce(operator.add)
	pprint(result)
	assert result == expected


if __name__ == '__main__':
	import nose
	nose.main()
