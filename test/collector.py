from multiprocessing import cpu_count
import os, sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from stream import ForkedFeeder, PCollector

## A trivial multiple producers -- single consumer test

N = 10000

def producer():
	for x in xrange(N):
		yield x

if __name__ == '__main__':
	consumer = PCollector()
	nProducer = cpu_count() - 1
	for _ in range(nProducer):
		ForkedFeeder(producer) >> consumer
	assert len(consumer) == N * nProducer
