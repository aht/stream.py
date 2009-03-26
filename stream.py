"""Lazy stream, with pipelining via the '>>' operator.

Anything iterable can be piped to a stream.  A stream can be piped to
any function taking one iterable argument.

>>> seq() >> map(lambda x: x**2) >> filter(lambda x: x%2 == 1) >> take(10)
[1, 9, 25, 49, 81, 121, 169, 225, 289, 361]

Don't forget to combine with itertools and other functional programming tools.
"""

#_______________________________________________________________________
#
# NOTE
#_______________________________________________________________________
#
#	* I choose the '>>' operator over the '|' of the UNIX pipe because
# it stands out better in Python code, is less used (e.g. '|' is used
# for set union), and binds more tightly.
#_______________________________________________________________________


# TODO:
#	* performance testing

__version__ = '0.2'
__author__ = 'Hai-Anh Trinh'
__email__ = 'moc.liamg@hnirt.iah.hna:otliam'[::-1]
__credits__ = 'Jakub Wilk, for the design from python-pipeline'
__all__ = [
	'Stream',
	'takei',
	'take',
	'dropi',
	'dropv',
	'map',
	'filter',
	'grep',
	'reduce',
	'apply',
	'pick',
	'flatten',
	'alternate',
	'zip',
	'tee',
	'prepend',
	'append',
	'count',
	'seq',
	'gseq',
]


import collections, itertools
import re

from functools import reduce as reduce_func

#_______________________________________________________________________
#
# Base class for stream processor
#_____________________________________________________________________

class Stream(collections.Callable, collections.Iterator):
	"""A class representing both a stream and a processor.

	The outgoing stream is represented by the attribute 'iterator'.

	The processor is represented by  the method __call__(other), which
	return a new Stream with a modified iterator attribute,
	representing a new outgoing stream.

	>>> s = Stream(seq(1))
	>>> s >> next
	1
	>>> s >> next
	2
	>>> s >> next
	3
	"""

	__slots__ = 'iterator',

	def __init__(self, *iterables):
		"""Make a stream object from an iterable"""
		self.iterator = iter([])
		if iterables:
			self.iterator = itertools.chain(*iterables)
		self.stream_repr = repr(iterables)

	def __call__(self, other):
		"""Implement iterator-combining mechanism here"""
		raise NotImplementedError

	def __iter__(self):
		if self.iterator:
			return self.iterator

	def __next__(self):
		if self.iterator:
			return next(self.iterator)

	def next(self):
		if self.iterator:
			return next(self.iterator)

	@staticmethod
	def __pipe(left, right):
		# Implement the overloading of  the operator '>>':
		# __pipe__(left, right) invoke right.__call__(left), and
		# depending on what it gets, return the appropiate object
		# (esp. if it is a Stream, __pipe__() make a good stream_repr)
		try:
			right.__call__
		except AttributeError:
			return Stream(iter(right))
		else:
			applied = right(left)
			if isinstance(applied, collections.Sized) \
				or isinstance(applied, collections.Container):
				return applied				### things more 'definite' than Stream
			elif isinstance(applied, Stream):
				# lrepr = left.stream_repr if isinstance(left, Stream) or repr(left)
				# rrepr = right.stream_repr if isinstance(right, Stream) or repr(right)
				# applied.stream_repr = lrepr + ' >> '  + rrepr
				if isinstance(left, Stream):
					applied.stream_repr = left.stream_repr + ' >> '  + right.stream_repr
				else:
					applied.stream_repr = repr(left) + ' >> '  + right.stream_repr
				return applied
			else:
				return applied

	def __rshift__(self, other):
		return Stream.__pipe(self, other)
	
	def __rrshift__(self, other):
		return Stream.__pipe(other, self)

	def __repr__(self):
		return '<Stream: %s>' % self.stream_repr


#_______________________________________________________________________
#
# Simple taking and dropping elements
#_______________________________________________________________________


class takei(Stream):
	"""Select elements of the incoming stream by a stream of indexes.

	>>> seq() >> takei(xrange(2,43,4)) >> list
	[2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42]

	>>> xrange(15) >> takei([3, -2, 7, 7]) >> list
	[3, 7]
	"""
	__slots__ = 'indexiter',

	def __init__(self, indexes):
		"""indexes should be non-negative integers in monotonically
		increasing order (bad values won't yield)
		"""
		self.iterator = iter([])
		self.indexiter = iter(indexes)
		self.stream_repr = 'takei(' + repr(indexes) + ')'

	def __call__(self, other):
		def genfunc():
			i = iter(other)
			old_idx = -1
			idx = self.indexiter.next()			# next value to yield
			counter = seq()
			while 1:
				c = counter.next()
				elem = i.next()
				while idx <= old_idx:		# ignore bad values
					idx = self.indexiter.next()
				if c == idx:
					yield elem
					old_idx = idx
					idx = self.indexiter.next()
		return Stream(genfunc())


class take(takei):
	"""Use slice-like notation to select elements.  Return a list.
	
	>>> seq(1, 2) >> take(10)
	[1, 3, 5, 7, 9, 11, 13, 15, 17, 19]

	>>> gseq(2) >> take(0, 16, 2)
	[1, 4, 16, 64, 256, 1024, 4096, 16384]
	"""
	def __init__(self, *args):
		"""The same as takei(xrange(*args)) >> list"""
		super(take, self).__init__(xrange(*args))

	def __call__(self, other):
		return super(take, self).__call__(other) >> list


class dropi(Stream):
	"""Drop elements of the incoming stream by indexes.

	>>> xrange(15) >> dropi([1, 2, 3, 5, 7, 11, 13]) >> list
	[0, 4, 6, 8, 9, 10, 12, 14]

	>>> xrange(11) >> dropi([-2, 3, 7, 7, 6, 9]) >> list
	[0, 1, 2, 4, 5, 6, 8, 10]

	>>> xrange(11) >> dropi([]) >> list
	[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
	"""
	__slot__ = 'indexiter'

	def __init__(self, indexes):
		"""indexes: a stream of the indexs of element to be selected.

		indexes should be non-negative integers in monotonically
		increasing order (bad values won't be discarded)
		"""
		self.iterator = iter([])
		self.indexiter = iter(indexes)
		self.stream_repr = 'dropi(' + repr(indexes) + ')'

	def __call__(self, other):
		def genfunc():
			i = iter(other)
			counter = seq()
			def try_next_idx():
				# so that the stream keeps going 
				# after the discard iterator is exhausted
				try:
					return self.indexiter.next(), False
				except StopIteration:		
					return -1, True
			old_idx = -1
			idx, exhausted = try_next_idx()			# next value to discard
			while 1:
				c =counter.next()
				elem = i.next()
				while not exhausted and idx <= old_idx:	# ignore bad values
					idx, exhausted = try_next_idx()	
				if c != idx:
					yield elem
				elif not exhausted:
					old_idx = idx
					idx, exhausted = try_next_idx()
		return Stream(genfunc())


class dropv(Stream):
	"""Drop elements of the incoming stream by values.

	>>> seq(1) >> dropv(seq(2, 2)) >> take(10)
	[1, 3, 5, 7, 9, 11, 13, 15, 17, 19]

	>>> seq(10) >> dropv(seq(2, 2)) >> take(10)
	[11, 13, 15, 17, 19, 21, 23, 25, 27, 29]
	"""
	__slot__ = 'valueiter',

	def __init__(self, values):
		"""indexes: a stream of the values of element to be dropped.

		Require both the incoming and the values streams to be in
		monotonically increasing order.
		"""
		self.iterator = iter([])
		self.valueiter = iter(values)
		self.stream_repr = 'dropv(' + repr(values) + ')'

	def __call__(self, other):
		def genfunc():
			i = iter(other)
			y = self.valueiter.next()
			while 1:
				x = i.next()
				while x>y:
					y = self.valueiter.next()
				if x==y:
					y = self.valueiter.next()
				elif x<y:
					yield x
		return Stream(genfunc())


#_______________________________________________________________________
#
# Functional processing
#_______________________________________________________________________


class FunctionFilter(Stream):
	"""Base class for stream filter based on a function"""

	__slots__ = 'function',

	def __init__(self, function):
		try:
			function.__call__
		except AttributeError:
			raise TypeError('function is not callable')
		self.iterator = iter([])
		self.function = function
		self.stream_repr = repr(function)


class takewhile(FunctionFilter):
	pass


class dropwhile(FunctionFilter):
	pass


class map(FunctionFilter):
	"""Map a function onto a stream. 

	>>> xrange(1,50,7) >> map(lambda x: x**2) >> list
	[1, 64, 225, 484, 841, 1296, 1849]
	"""
	__slots__ = 'function',

	def __init__(self, function):
		super(map, self).__init__(function)
		self.stream_repr = 'map(%s)' % repr(function)

	def __call__(self, other):
		return Stream(itertools.imap(self.function, other))


class filter(FunctionFilter):
	"""Filter a stream by a function, taking only elements for which
	the function evaluate to True.

	>>> even = lambda x: x%2 == 0
	>>> xrange(1, 40, 3) >> filter(even) >> list
	[4, 10, 16, 22, 28, 34]
	"""
	def __init__(self, function=None):
		super(filter, self).__init__(function)
		self.stream_repr = 'filter(%s)' % repr(function)

	def __call__(self, other):
		return Stream(itertools.ifilter(self.function, other))


class grep(filter):
	pass


class reduce(FunctionFilter):
	"""scanl"""
	def __init__(self, function=None):
		super(reduce, self).__init__(function)
		self.stream_repr = 'reduce(%s)' + repr(function)

	def __call__(self, other):
		def genfunc():
			i = other.iterator
			while 1:	
				x = i.next()
				y = i.next()
				yield self.function(x, y)
		return Stream(genfunc())

#_______________________________________________________________________
#
# Stream combinators
#_____________________________________________________________________


class prepend(Stream):
	"""Prepend at the beginning of a stream.

	>>> seq(7, 7) >> prepend(xrange(0, 10, 2)) >> take(10)
	[0, 2, 4, 6, 8, 7, 14, 21, 28, 35]
	"""
	def __init__(self, *iterables):
		"""iterables: streams to be prepended"""
		self.iterator = iter([])
		if iterables:
			self.iterator = itertools.chain(*iterables)
		self.stream_repr = 'prepend' + repr(iterables)

	def __call__(self, other):
		"""Prepend at the beginning of other"""
		s = Stream(other)
		if self.iterator:
			s.iterator = itertools.chain(self.iterator, s.iterator)
		return s


class append(Stream):
	"""Append to the end of a stream (it had better terminate!)

	>>> xrange(1, 20, 7) >> append(xrange(1, 10, 3), 'foo') >> list
	[1, 8, 15, 1, 4, 7, 'f', 'o', 'o']
	"""
	def __init__(self, *iterables):
		"""iterables: streams to be appended"""
		self.iterator = iter([])
		if iterables:
			self.iterator = itertools.chain(*iterables)
		self.stream_repr = 'append' + repr(iterables)

	def __call__(self, other):
		"""Append to the end of other"""
		s = Stream(other)
		if self.iterator:
			s.iterator = itertools.chain(s.iterator, self.iterator)
		return s


class tee(Stream):
	"""Make a branch of a stream"""

	__slots__ = 'streamobj',

	def __init__(self, streamobj):
		self.streamobj = streamobj
		self.stream_repr = 'tee(%s)' % repr(streamobj)

	def __call__(self, other):
		"""Modify streamobj to refer to a copy of iterable
		"""
		####################################
		### BUG: Python can't copy generator
		###
		### Itertools.tee is fine
		### Somehow it doesn't work with Stream 
		####################################
		self.streamobj.iterator, = itertools.tee(other, 1)

		if isinstance(other, Stream):
			self.streamobj.stream_repr = other.stream_repr
		else:
			self.streamobj.stream_repr = repr(other)
		return Stream(other)


class alternate(Stream):
	"""The simplest of all merge"""
	pass


class zip(Stream):
	pass


#_______________________________________________________________________
#
# Processing nested streams
#_____________________________________________________________________


class apply(Stream):
	pass


class pick(Stream):
	pass


class flatten(Stream):
	pass


class count(Stream):
	"""Count the number of elements of a stream"""
	pass


#_______________________________________________________________________
#
# Useful generators
#_____________________________________________________________________


def seq(start=0, step=1):
	"""An arithmetic sequence generator.  Works with any type with + defined.

	>>> seq(1, 0.25) >> take(10)
	[1, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5, 2.75, 3.0, 3.25]
	"""
	def genfunc(a, d):
		while 1:
			yield a
			a += d
	return genfunc(start, step)


def gseq(*args):
	"""A geometric sequence generator.  Works with any type with * defined.

	>>> from decimal import Decimal
	>>> gseq(Decimal('.2')) >> take(4)
	[1, Decimal('0.2'), Decimal('0.04'), Decimal('0.008')]
	"""
	def genfunc(a, r):
		while 1:
			yield a
			a *= r
	if len(args) == 1:
		return genfunc(1, args[0])
	elif len(args) == 2:
		return genfunc(args[0], args[1])
	else:
		raise TypeError('gseq expects 1 or 2 arguments, got %s' % len(args))


if __name__ == "__main__":
	import doctest
	doctest.testmod()
