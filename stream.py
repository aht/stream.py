"""Iterable stream with UNIX pipe notation.

Add immense power and elegance to Python's list processing, especially
when used in a functional style.  Remember to combine with itertools also!

>>> sum_divisors = lambda n: xrange(1, n//2 + 1) >> filter(lambda i: n%i==0) >> sum
>>> perfect = lambda n: sum_divisors(n) == n
>>> itertools.count() >> filter(perfect) | select(range(4)) >> list
[6, 28, 496, 8128]
"""

__version__ = '0.2'
__author__ = 'Hai-Anh Trinh'
__email__ = 'moc.liamg@hnirt.iah.hna:otliam'[::-1]
__credits__ = 'Jakub Wilk, for the python-pipeline package'
__all__ = [
	'Stream',
	'select', 'discard', 'tee',
	'map', 'filter', 'grep', 'process_stream',
	'pick', 'flatten', 'merge',
	'cat', 'reduce', 'sort', 'count'		# requires terminating stream
]


import collections, itertools
import operator
import re

from functools import reduce as reduce_func

class Stream(collections.Iterable):
	"""Base class for streams and filters"""

	__slots__ = 'iterator',

	def __init__(self, iterable=None):
		"""Make a stream object from an iterable"""
		if iterable:
			self.iterator = iter(iterable)
			self.stream_repr = repr(iterable)

	def __call__(self):
		"""Implement iterator-combining mechanism here"""
		raise NotImplementedError

	def __iter__(self):
		if self.iterator:
			return self.iterator

	@staticmethod
	def __pipe(left, right):
		try:
			right.__call__
		except AttributeError:
			return Stream(iter(right))
		else:
			applied = right(left)
			if isinstance(right, type):
				return applied
			elif isinstance(applied, Stream):
				if isinstance(left, Stream):
					applied.stream_repr = left.stream_repr + ' | '  + right.stream_repr
				else:
					applied.stream_repr = repr(left) + ' | '  + right.stream_repr
				return applied
			else:
				try:
					return Stream(applied)
				except TypeError:		### not an iterable anymore
					return applied

	def __or__(self, other):
		return Stream.__pipe(self, other)
	
	def __ror__(self, other):
		return Stream.__pipe(other, self)

	def __repr__(self):
		return '<Stream: %s>' % self.stream_repr


class sliced(Stream):
	"""Slice the stream using the usual list notation"""
	# select in python-pipeline


class select(Stream):
	"""Select elements of a stream by their indexes

	>>> from itertools import count
	>>> count() | select(xrange(2,43,4)) | list
	[2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42]

	>>> xrange(15) | select([3, -2, 7, 7]) | list
	[3, 7]
	"""
	__slots__ = 'indexes',

	def __init__(self, indexes):
		"""indexes: a stream of the indexs of element to be selected.

		indexes should be non-negative integers in monotonically
		increasing order (bad values won't yield)
		"""
		self.indexes = indexes
		self.iterator = iter(indexes)
		self.stream_repr = 'select(' + repr(indexes) + ')'

	def __call__(self, iterable):
		def genfunc():
			i = iter(iterable)
			old_idx = -1
			idx = self.iterator.next()			# next value to yield
			counter = itertools.count()
			while 1:
				c = counter.next()
				elem = i.next()
				while idx <= old_idx:		# ignore bad values
					idx = self.iterator.next()
				if c == idx:
					yield elem
					old_idx = idx
					idx = self.iterator.next()
		return Stream(genfunc())


class discard(Stream):
	"""Discard elements of a stream by their indexes

	>>> xrange(15) | discard([1, 2, 3, 5, 7, 11, 13]) | list
	[0, 4, 6, 8, 9, 10, 12, 14]

	>>> xrange(11) | discard([-2, 3, 7, 7, 6, 9]) | list
	[0, 1, 2, 4, 5, 6, 8, 10]

	>>> xrange(11) | discard([]) | list
	[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
	"""
	__slot__ = 'indexes'

	def __init__(self, indexes):
		"""indexes: a stream of the indexs of element to be selected.

		indexes should be non-negative integers in monotonically
		increasing order (bad values won't be discarded)
		"""
		self.indexes = indexes
		self.iterator = iter(indexes)
		self.stream_repr = 'select(' + repr(indexes) + ')'

	def __call__(self, iterable):
		def genfunc():
			i = iter(iterable)
			counter = itertools.count()

			def try_next_idx():
				# so that the stream keeps going 
				# after the discard iterator is exhausted
				try:
					return self.iterator.next(), False
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


class map(Stream):
	"""Map a function onto a stream. 

	>>> xrange(1,50,7) | map(lambda x: x**2) | list
	[1, 64, 225, 484, 841, 1296, 1849]
	"""
	__slots__ = 'function',

	def __init__(self, function):
		try:
			function.__call__
		except AttributeError:
			raise TypeError('function is not callable')
		self.function = function
		self.stream_repr = 'map(' + repr(function) + ')'

	def __call__(self, iterable):
		"""Return the stream of values returned  by function."""
		return Stream(itertools.imap(self.function, iterable))


class filter(Stream):
	"""Filter a stream by a function.

	>>> even = lambda x: x%2 == 0
	>>> xrange(1, 40, 3) | filter(even) | list
	[4, 10, 16, 22, 28, 34]
	"""
	__slots__ = 'function',

	def __init__(self, function=None):
		try:
			function.__call__
		except AttributeError:
			if function != None:
				raise TypeError('function is not callable')
		self.function = function
		self.stream_repr = 'filter(' + repr(function) + ')'

	def __call__(self, iterable):
		return Stream(itertools.ifilter(self.function, iterable))


class cat(Stream):
	"""Similar to the beloved UNIX namesake utility.  
	Make sure the first stream is finite!

	>>> xrange(1, 20, 7) | cat(xrange(1, 10, 3), 'foo') | list
	[1, 8, 15, 1, 4, 7, 'f', 'o', 'o']
	"""
	def __init__(self, *iterables):
		"""Concatenate iterables to form a stream"""
		self.iterator = None
		if iterables:
			self.iterator = itertools.chain(*iterables)
		self.stream_repr = 'cat(' + repr(iterables) + ')'

	def __call__(self, iterable):
		s = cat(iterable)
		if self.iterator:
			s.iterator = itertools.chain(s.iterator, self.iterator)
		s.stream_repr = 'cat' + (repr(iterable) or '()')
		return s


if __name__ == "__main__":
	import doctest
	doctest.testmod()