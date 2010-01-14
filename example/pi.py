#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

import operator

from decimal import Decimal, getcontext
from stream import Stream, Processor, seq, gseq, apply, map, fold, zip, item, drop

"""
Compute digits of pi using the Gregory series, and its accelerated variants.

Inspired by this section of the wizard book (SICP):
<http://mitpress.mit.edu/sicp/full-text/sicp/book/node72.html>
"""

def alt_sign(s):
	"""Alternate the sign of numbers of the input stream by multiply it with
	the unit alternating series 1, -1, ...
	"""
	return zip(s, gseq(-1, initval=1)) >> apply(operator.mul)


def Gregory(type=float):
	"""Return partial sums of the Gregory series converging to atan(1) == pi/4.

	Yield 1 - 1/3 + 1/5 - 1/7 + ... computed with the given type.
	"""
	return seq(type(1), step=2) >> map(lambda x: 1/x) >> alt_sign >> fold(operator.add)


series1 = Gregory()


@Processor
def Aitken(s):
	"""Accelerate the convergence of the a series
	using Aitken's delta-squared process (SCIP calls it Euler).
	"""
	def accel():
		s0, s1, s2 = s >> item[:3]
		while 1:
			yield s2 - (s2 - s1)**2 / (s0 - 2*s1 + s2)
			s0, s1, s2 = s1, s2, next(s)
	return accel()


series2 = Gregory() >> Aitken


class recur_transform(Stream):
	"""Apply a accelerator recursively."""
	def __init__(self, accelerator):
		super(recur_transform, self).__init__()
		self.accelerator = accelerator
	
	def __call__(self, series):
		def transform():
			s = series
			while 1:
				yield next(s)
				s = iter(s >> self.accelerator)
		return transform()


series3 = Gregory(Decimal) >> recur_transform(Aitken)

# The effect of the recursive transformation is:
#   i = iter(series3)
#   next(i) == Gregory(Decimal) >> item[0]
#   next(i) == Gregory(Decimal) >> drop(1) >> Aitken >> item[0]
#   next(i) == Gregory(Decimal) >> drop(1) >> Aitken >> drop(1) >> Aitken >> item[0]
#   next(i) == Gregory(Decimal) >> drop(1) >> Aitken >> drop(1) >> Aitken >> drop(1) >> Aitken >> item[0]
#   ...
# Note that it is possible to account for the dropped values using
# stream.prepend, but it's not necessary.


if __name__ == '__main__':
	getcontext().prec = 33
	print 'π =', 4 * (series3 >> item[13])
