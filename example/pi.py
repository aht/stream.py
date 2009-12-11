#!/usr/bin/env python2.6

import operator

from decimal import Decimal
from stream import Stream, Filter, seq, gseq, apply, map, fold, zipwith, item

"""
Compute digits of pi using the Gregory series, and its accelerated variants.

For a detailed explanation, see this section of SICP:
<http://mitpress.mit.edu/sicp/full-text/sicp/book/node72.html>
"""

alt_sign = lambda s: s >> zipwith(gseq(1, -1)) >> apply(operator.mul)

## Return the partial sums of the Gregory series converging to atan(1) == pi/4
## @param t: the type of number we want to work with: either float or Decimal
def Gregory(t=float):
	return seq(t(1), step=2) >> map(lambda x: 1/x) >> alt_sign >> fold(operator.add)

series1 = Gregory()

## Accelerate the convergence by transforming the series
## using Aitken's delta-squared process (SCIP calls it Euler)
@Filter
def Aitken(inpipe):
	def genfunc():
		s0, s1, s2 = inpipe >> item[:3]
		while 1:
			yield s2 - (s2 - s1)**2 / (s0 - 2*s1 + s2)
			s0, s1, s2 = s1, s2, next(inpipe)
	return genfunc()

series2 = Gregory() >> Aitken

## Recursively apply a transformation
class recur_transform(Stream):
	def __init__(self, accelerator):
		super(recur_transform, self).__init__()
		self.accelerator = accelerator
	
	def __call__(self, inpipe):
		def genfunc():
			s = inpipe
			while 1:
				yield next(s)
				s = s >> self.accelerator
		return genfunc()

series3 = Gregory(Decimal) >> recur_transform(Aitken)
