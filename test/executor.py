#!/usr/bin/env python

import os
import threading
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from stream import map, Executor, ProcessPool, ThreadPool


result = {
	100: 328350,
	1000: 332833500,
	10000: 333283335000,
}


## Test submission and results, for sanity.

def submit(poolclass, n):
	e = Executor(poolclass, map(lambda x: x*x), poolsize=3)
	e.submit(*range(n))
	e.close()
	assert sum(e.result) == result[n]

def test_threadpool_submit():
	for n in result.keys():
		yield submit, ThreadPool, n

def test_procpool_submit():
	for n in result.keys():
		yield submit, ProcessPool, n


## Test concurrent submission and cancellation

def cancel(poolclass, n):
	e = Executor(poolclass, map(lambda x: x*x), poolsize=2)
	t1 = threading.Thread(target=lambda: e.submit(*range(n//2)))
	t2 = threading.Thread(target=lambda: e.submit(*range(n//2)))
	t1.start()
	t2.start()
	cancelled = e.cancel(*range(0, n, 2))
	t1.join()
	t2.join()
	e.close()
	completed = len(e.result >> list)
	print completed, cancelled
	assert completed + cancelled == n

def test_threadpool_cancel():
	for n in result.keys():
		yield cancel, ThreadPool, n

def test_procpool_cancel():
	for n in result.keys():
		yield cancel, ProcessPool, n


## Test shutdown

def shutdown(poolclass, n):
	e = Executor(poolclass, map(lambda x: x*x), poolsize=2)
	e.submit(*range(n))
	e.shutdown()
	print e.result >> list
	assert e.inputfeeder_thread.is_alive() == False
	assert e.resulttracker_thread.is_alive() == False
	assert e.failuretracker_thread.is_alive() == False

def test_threadpool_shutdown():
	for n in result.keys():
		yield shutdown, ThreadPool, n

def test_procpool_shutdown():
	for n in result.keys():
		yield shutdown, ProcessPool, n


if __name__ == "__main__":
	import nose
	nose.main()
