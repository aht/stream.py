#!/usr/bin/env python2.6

import os
import sys

from pprint import pprint
from random import randint

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from stream import filter, map, ThreadPool, ProcessPool


## The test data

dataset = []

def alternating(n):
	values = []
	for i in range(1, n+1):
		values.append(i)
		values.append(-i)
	return values

def randomized(n):
	values = []
	for _ in range(n):
		values.append(randint(-sys.maxint, sys.maxint))
	return values

for v in [10, 100, 1000] >> map(alternating):
	dataset.append(v)

for v in [10, 100, 1000] >> map(randomized):
	dataset.append(v)

func = filter(lambda x: x&1)

resultset = dataset >> map(lambda s: s >> func >> set) >> list


## Test scenario

def threadpool(i):
	result = dataset[i] >> ThreadPool(func, poolsize=2) >> set
	pprint(result)
	assert result == resultset[i]

def processpool(i):
	result = dataset[i] >> ProcessPool(func, poolsize=2) >> set
	pprint(result)
	assert result == resultset[i]


## Test cases

def test_ThreadPool():
	for i in range(len(dataset)):
		yield threadpool, i

def test_ProcessPool():
	for i in range(len(dataset)):
		yield processpool, i


if __name__ == '__main__':
	import nose
	nose.main()
