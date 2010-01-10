#!/usr/bin/env python

from stream import map, Executor, ProcessPool, ThreadPool

result = {
	100: 328350,
	1000: 332833500,
	10000: 333283335000,
}

def sum_squares(poolclass, n):
	e = Executor(poolclass, map(lambda x: x*x))
	for i in range(n):
		e.submit(i)
	e.finish()
	assert sum(e.result) == result[n]

def test_threadpool():
	for n in result.keys():
		yield sum_squares, ThreadPool, n

def test_processpool():
	for n in result.keys():
		yield sum_squares, ProcessPool, n

if __name__ == "__main__":
	import nose
	nose.main()
