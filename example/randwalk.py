#!/usr/bin/env python

import operator

from random import choice
from stream import repeatcall, apply, fold, takei, takewhile, drop, item


vectoradd = lambda u, v: zip(u, v) >> apply(operator.add) >> list

Origin = [0, 0]
Directions = [[1,0], [0,1], [-1,0], [0,-1]]

randwalk = lambda: repeatcall(choice, Directions) >> fold(vectoradd, initval=Origin)

def returned(n):
	"""Generate a random walk and return True if the walker has returned to
	the origin after taking `n` steps.
	"""
	## `takei` yield lazily so we can short-circuit and avoid computing the rest of the walk
	for pos in randwalk() >> drop(1) >> takei(xrange(n-1)):
		if pos == Origin:
			return True
	return False

def first_return():
	"""Generate a random walk and return its length upto the moment
	that the walker first returns to the origin.

	It is mathematically provable that the walker will eventually return,
	meaning that the function call will halt, although it may take
	a *very* long time and your computer may run out of memory!
	Thus, try this interactively only.
	"""
	walk = randwalk() >> drop(1) >> takewhile(lambda v: v != Origin) >> list
	return len(walk)


if __name__ == '__main__':
	r10k = sum(1 for _ in range(100) if returned(10000))
	print "Out of 100 times the walker takes 10000 steps, " \
		+ "%s times he has returned to the origin." % r10k
