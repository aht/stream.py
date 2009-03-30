"""Lazy stream, with pipelining via the '>>' operator.

Anything iterable can be piped to a stream.  A stream can be piped to
any function taking one iterable argument.

>>> seq() >> map(lambda x: x**2) >> filter(lambda x: x%2 == 1) >> take(10) >> sum
1330

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


#_______________________________________________________________________
#
# PERFORMANCE
#_______________________________________________________________________
#
#	* itertools.count() is 150% faster than seq(), no doubt due to
# the former implementation in C.
#_______________________________________________________________________

__version__ = '0.2'
__author__ = 'Hai-Anh Trinh'
__email__ = 'moc.liamg@hnirt.iah.hna:otliam'[::-1]
__credits__ = 'Jakub Wilk, for the design from python-pipeline'
__all__ = [
	'Stream',
	'takei',
	'take',
	'item',
	'dropi',
	'dropv',
	'map',
	'filter',
	'grep',
	'reduce',
	'apply',
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

class Stream(object):
	"""A class representing both a stream and a processor.

	The outgoing stream is represented by the attribute 'iterator'.

	The processor is represented by  the method __call__(inpipe), which
	combine iterator attribute,

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

	def __init__(self, iterable=None):
		"""Make a stream object from an iterable"""
		self.iterator = iter(iterable if iterable else [])

	def __iter__(self):
		return self.iterator

	def __next__(self):
		return next(self.iterator)

	def next(self):
		return next(self.iterator)

	def __pipe__(self, inpipe):
		self.iterator = self.__call__(inpipe)
		return self

	def __extend__(self, inpipe):
		inpipe.iterator = self.__call__(inpipe)
		return inpipe

	def __rshift__(self, other):
		return Stream.__pipe(self, other)

	def __rrshift__(self, other):
		return Stream.__pipe(other, self)

	@staticmethod
	def __pipe(left, right):
		# Implement the overloading of  the operator '>>'
		if hasattr(right, '__pipe__'):
			func = right.__pipe__
		elif hasattr(right, '__call__'):
			func = right.__call__
		else:						### no connection mechanism exists
			return right if isinstance(right, Stream) else Stream(right)
		return func(left)

	def __lshift__(self, other):
		return Stream.__extend(self, other)

	@staticmethod
	def __extend(left, right):
		# Implement the overloading of  the operator '<<'
		if hasattr(right, '__extend__'):
			func = right.__extend__
		else:						### no connection mechanism exists
			return right if isinstance(right, Stream) else Stream(right)
		return func(left)

	def __repr__(self):
		return 'Stream(%s)' % repr(self.iterator)


#_______________________________________________________________________
#
# Simple taking and dropping elements
#_______________________________________________________________________


negative = lambda x: x and x<0		### since None < 0 == True


class take(Stream):
	"""Force some or all evaluation and use slice-like arguments to select elements.
	Return a Stream.
	
	>>> seq(1, 2) >> take(10)
	Stream([1, 3, 5, 7, 9, 11, 13, 15, 17, 19])

	>>> gseq(2) >> take(0, 16, 2)
	Stream([1, 4, 16, 64, 256, 1024, 4096, 16384])
	"""
	__slots__ = 'slice', 'cache'

	def __init__(self, *args):
		self.iterator = iter([])
		self.slice = slice(*args)

	def __call__(self, inpipe):
		if negative(self.slice.stop) or negative(self.slice.start) \
		or not (self.slice.start or self.slice.stop) \
		or (not self.slice.start and negative(self.slice.step)) \
		or not (self.slice.stop or negative(self.slice.step)):
			self.cache = list(inpipe)		## force all evaluation 	##
		else:							## force some evaluation ##
			if not self.slice.step or self.slice.step > 0:
				stop = self.slice.stop
			else:
				stop = self.slice.start
			i = iter(inpipe)
			try:
				self.cache =  [next(i) for _ in xrange(stop)]
			except StopIteration:
				pass
		self.cache = self.cache[self.slice]
		return iter(self.cache)

	def __pipe__(self, inpipe):
		self.iterator = self.__call__(inpipe)
		return self

	def __extend__(self, inpipe):
		inpipe.iterator = self.__call__(inpipe)
		return inpipe

	def __repr__(self):
		return 'Stream(%s)' % repr(self.cache)


class itemgetter(take):
	"""
	Implement Python slice notation for take. Return a list.
	
	>>> xrange(20) >> item[::-2]
	[19, 17, 15, 13, 11, 9, 7, 5, 3, 1]
	"""
	__slots__ = 'get1'

	def __init__(self):
		self.iterator = iter([])
		self.get1 = False

	@classmethod
	def __getitem__(cls, sliceobj):
		s = cls()
		if type(sliceobj) is type(1):
			s.get1 = True
			s.slice = slice(sliceobj)
		else:
			s.slice = sliceobj
		return s

	def __pipe__(self, inpipe):
		super(itemgetter, self).__call__(inpipe)
		if self.get1:
			return self.cache[-1]
		else:
			return self.cache

	def __repr__(self):
		return '<itemgetter at %s>' % hex(id(self))

item = itemgetter()


class takei(Stream):
	"""Select elements of the incoming stream by a stream of indexes.

	>>> seq() >> takei(xrange(2,43,4)) >> list
	[2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42]

	>>> xrange(15) >> takei([3, -2, 7, 7]) >> list
	[3, 7]
	"""
	__slots__ = 'indexiter',

	def __init__(self, indices):
		"""indexes should be non-negative integers in monotonically
		increasing order (bad values won't yield)
		"""
		self.iterator = iter([])
		self.indexiter = iter(indices)

	def __call__(self, other):
		def genfunc():
			i = iter(other)
			old_idx = -1
			idx = next(self.indexiter)			# next value to yield
			counter = seq()
			while 1:
				c = next(counter)
				elem = next(i)
				while idx <= old_idx:		# ignore bad values
					idx = next(self.indexiter)
				if c == idx:
					yield elem
					old_idx = idx
					idx = next(self.indexiter)
		return genfunc()


class dropi(Stream):
	"""Drop elements of the incoming stream by indexes.

	>>> seq() >> dropi(seq(0,3)) >> item[:10]
	[1, 2, 4, 5, 7, 8, 10, 11, 13, 14]

	>>> xrange(11) >> dropi([-2, 3, 7, 7, 6, 9]) >> list
	[0, 1, 2, 4, 5, 6, 8, 10]

	>>> xrange(11) >> dropi([]) >> list
	[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
	"""
	__slot__ = 'indexiter'

	def __init__(self, indices):
		"""indexes: a stream of the indexs of element to be selected.

		indexes should be non-negative integers in monotonically
		increasing order (bad values won't be discarded)
		"""
		self.iterator = iter([])
		self.indexiter = iter(indices)

	def __call__(self, other):
		def genfunc():
			i = iter(other)
			counter = seq()
			def try_next_idx():
				## so that the stream keeps going 
				## after the discard iterator is exhausted
				try:
					return next(self.indexiter), False
				except StopIteration:		
					return -1, True
			old_idx = -1
			idx, exhausted = try_next_idx()			# next value to discard
			while 1:
				c =next(counter)
				elem = next(i)
				while not exhausted and idx <= old_idx:	# ignore bad values
					idx, exhausted = try_next_idx()	
				if c != idx:
					yield elem
				elif not exhausted:
					old_idx = idx
					idx, exhausted = try_next_idx()
		return genfunc()


class dropv(Stream):
	"""Drop elements of the incoming stream by values.

	>>> seq(1) >> dropv(seq(2, 2)) >> item[:10]
	[1, 3, 5, 7, 9, 11, 13, 15, 17, 19]

	>>> seq(10) >> dropv(seq(2, 2)) >> item[:10]
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

	def __call__(self, other):
		def genfunc():
			i = iter(other)
			y = next(self.valueiter)
			while 1:
				x = next(i)
				while x>y:
					y = next(self.valueiter)
				if x==y:
					y = next(self.valueiter)
				elif x<y:
					yield x
		return genfunc()


#_______________________________________________________________________
#
# Functional processing
#_______________________________________________________________________


class FunctionFilter(Stream):
	"""Base class for stream filter based on a function"""

	__slots__ = 'function',

	def __init__(self, function):
		if not hasattr(function, '__call__'):
			raise TypeError("'%s' object is not callable" % function)
		self.iterator = iter([])
		self.function = function


class apply(FunctionFilter):
	"""itertools.starmap"""
	pass


class count(apply):
	"""Count the number of elements of a stream"""
	pass


class map(FunctionFilter):
	"""Map a function onto a stream. 

	>>> xrange(1,50,7) >> map(lambda x: x**2) >> list
	[1, 64, 225, 484, 841, 1296, 1849]
	"""
	def __call__(self, other):
		return itertools.imap(self.function, other)


class filter(FunctionFilter):
	"""Filter a stream by a function, taking only elements for which
	the function evaluate to True.

	>>> even = lambda x: x%2 == 0
	>>> xrange(1, 40, 3) >> filter(even) >> list
	[4, 10, 16, 22, 28, 34]
	"""
	def __init__(self, function=None):
		try:
			super(filter, self).__init__(function)
		except TypeError:
			pass

	def __call__(self, other):
		return itertools.ifilter(self.function, other)


class takewhile(FunctionFilter):
	pass


class dropwhile(FunctionFilter):
	pass


class grep(filter):
	pass


class reduce(FunctionFilter):
	"""Haskell's scanl"""
	def __call__(self, other):
		def genfunc():
			i = other.iterator
			while 1:	
				x = next(i)
				y = next(i)
				yield self.function(x, y)
		return genfunc()

#_______________________________________________________________________
#
# Stream combinators
#_____________________________________________________________________


class prepend(Stream):
	"""Prepend at the beginning of a stream.

	>>> seq(7, 7) >> prepend(xrange(0, 10, 2)) >> item[:10]
	[0, 2, 4, 6, 8, 7, 14, 21, 28, 35]
	"""
	def __call__(self, inpipe):
		"""Prepend at the beginning of other"""
		if self.iterator:
			return itertools.chain(self.iterator, iter(inpipe))
		else:
			return iter(inpipe)


class append(Stream):
	"""Append to the end of a stream (it had better terminate!)

	>>> xrange(1, 20, 7) >> append(xrange(1, 10, 3)) >> append('foo') >> list
	[1, 8, 15, 1, 4, 7, 'f', 'o', 'o']
	"""
	def __call__(self, inpipe):
		if self.iterator:
			return itertools.chain(iter(inpipe), self.iterator)
		else:
			return iter(inpipe)


class tee(Stream):
	"""Make a branch of a stream"""

	__slots__ = 'streamobj',

	def __init__(self, streamobj):
		self.streamobj = streamobj
		self.iterator = iter([])

	def __pipe__(self, inpipe):
		"""Modify streamobj to refer to a copy of iterable.
		Does not always work, see PEP-232.
		"""
		inpipe.iterator, self.streamobj.iterator = itertools.tee(inpipe)
		if isinstance(inpipe, Stream):
			return inpipe
		else:
			return Stream(inpipe)


class alternate(Stream):
	pass


class zip(Stream):
	pass


#_______________________________________________________________________
#
# Nested streams processing
#_______________________________________________________________________


class flattener(Stream):
	"""Flatten a nested stream of arbitrary depth"""
	@staticmethod
	def __call__(inpipe):
		def genfunc():
			stack = []
			i = iter(inpipe)
			while 1:
				try:
					e = next(i)
					if hasattr(e, "__iter__") and not isinstance(e, basestring):
						stack.append(i)
						i = iter(e)
					else:
						yield e
				except StopIteration:
					try:
						i = stack.pop()
					except IndexError:
						return
		return genfunc()

	def __repr__(self):
		return '<flattener at %s>' % hex(id(self))

flatten = flattener()


#_______________________________________________________________________
#
# Useful generators
#_____________________________________________________________________


def seq(start=0, step=1):
	"""An arithmetic sequence generator.  Works with any type with + defined.

	>>> seq(1, 0.25) >> item[:10]
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
	>>> gseq(Decimal('.2')) >> item[:4]
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

def fixpoint(func, initval):
	x = initval
	while 1:
		yield x
		x = func(x)


if __name__ == "__main__":
	import doctest
	doctest.testmod()
