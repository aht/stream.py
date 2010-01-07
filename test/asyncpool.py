#!/usr/bin/env python2.6

import os
import sys

from pprint import pprint
from random import randint

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from stream import filter, AsyncThreadPool, AsyncProcessPool


func = lambda s: s >> filter(lambda x: x&1)

dataset = []

def alternating(n):
	values = []
	for i in range(1, n+1):
		values.append(i)
		values.append(-i)
	return values

for v in map(alternating, [10, 100, 1000]):
	dataset.append(v)

def randomized(n):
	values = []
	for _ in range(n):
		values.append(randint(-sys.maxint, sys.maxint))
	return values

for v in map(randomized, [10, 100, 1000]):
	dataset.append(v)

def threadpool(dataset):
	a = dataset >> AsyncThreadPool(func, poolsize=2) >> set
	b = func(dataset) >> set
	pprint(a)
	pprint(b)
	assert a == b

def procpool(dataset):
	a = dataset >> AsyncProcessPool(func, poolsize=2) >> set
	b = func(dataset) >> set
	pprint(a)
	pprint(b)
	assert a == b

def test_threadpool():
	for v in dataset:
		yield threadpool, v

def test_procpool():
	for v in dataset:
		yield procpool, v

if __name__ == '__main__':
	import nose
	nose.main()
