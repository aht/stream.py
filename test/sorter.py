#!/usr/bin/env python2.6

import os, sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from stream import seq, take, item, ForkedFeeder, Sorter

if __name__ == '__main__':
	sorter = Sorter()

	ForkedFeeder(lambda: seq() >> take(10)) >> sorter
	ForkedFeeder(lambda: seq(step=2) >> take(10)) >> sorter

	sorter.run()

	assert sorter >> list == [0, 0, 1, 2, 2, 3, 4, 4, 5, 6, 6, 7, 8, 8, 9, 10, 12, 14, 16, 18]
