#!/usr/bin/env python2.6

import os, sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from stream import ForkedFeeder, ThreadedFeeder, PSorter, QSorter


def test_PSorter():
	sorter = PSorter()
	ForkedFeeder(lambda: iter(xrange(10))) >> sorter
	ForkedFeeder(lambda: iter(xrange(0, 20, 2))) >> sorter
	assert sorter >> list == [0, 0, 1, 2, 2, 3, 4, 4, 5, 6, 6, 7, 8, 8, 9, 10, 12, 14, 16, 18]

def test_QSorter():
	sorter = QSorter()
	ThreadedFeeder(lambda: iter(xrange(10))) >> sorter
	ThreadedFeeder(lambda: iter(xrange(0, 20, 2))) >> sorter
	assert sorter >> list == [0, 0, 1, 2, 2, 3, 4, 4, 5, 6, 6, 7, 8, 8, 9, 10, 12, 14, 16, 18]


if __name__ == '__main__':
	import nose
	nose.main()
