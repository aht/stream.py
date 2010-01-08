#!/usr/bin/env python2.6

import os, sys

from pprint import pprint

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from stream import ForkedFeeder, PCollector


## A trivial multiple producers -- single consumer test

N = 1000

def producer():
	for x in xrange(N):
		yield x

def collector(n):
	consumer = PCollector()
	for _ in range(n):
		ForkedFeeder(producer) >> consumer
	results = consumer >> list
	pprint(results)
	assert len(results) == N * n
	assert set(results) == set(xrange(N))

def test_collector():
	for i in [1, 2, 3, 4]:
		yield collector, i


if __name__ == '__main__':
	import nose
	nose.main()
