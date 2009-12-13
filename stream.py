"""Lazily-evaluated stream with pipelining via the '>>' operator

Introduction
============

Streams are generalized iterators with a pipelining mechanism to enable
data-flow programming.

The idea is to take the output of a function that turn an iterable into
another iterable and plug that as the input of another such function.
While you can already do this using function composition, this package
provides an elegant notation for it by overloading the '>>' operator.

This approach focuses the programming on processing streams of data, step
by step.  A pipeline usually starts with a producer, then passes through
a number of filters.  Multiple streams can be branched and combined.
Finally, the output is fed to an accumulator, which can be any function
of one iterable argument.

**Producers**:  anything iterable
	+ from this module:  seq, gseq, repeatcall, chaincall

**Filters**:
	+ by index:  take, drop, cut
	+ by condition:  filter, takewhile, dropwhile
	+ by transformation:  map, apply, fold
	+ special purpose:  attrgetter, itemgetter, methodcaller, splitter

**Combinators**:  prepend, takei, dropi, tee, flatten

**Accumulators**:  item, maximum, minimum, reduce
	+ from Python:  list, sum, dict, max, min ...

Values are computed only when an accumulator forces some or all evaluation
(not when the stream are set up).

Examples
========

Better itertools.islice
-----------------------
::

  >>> from itertools import count
  >>> c = count()
  >>> c >> item[1:10:2]
  [1, 3, 5, 7, 9]
  >>> c >> item[:5]
  [10, 11, 12, 13, 14]

String processing
-----------------
Grep some lines matching a regex from a file, cut out the 4th field
separated by ' ', ':' or '.', strip leading zeroes, then save as a list::

    import re
    s = open('file') \
      >> filter(re.compile(regex).search) \
      >> map(splitter(' |:|\.')) \
      >> map(itemgetter(3)) \
      >> map(methodcaller('lstrip', '0')) \
      >> list

Partial sums
------------
Compute the first few partial sums of the geometric series 1 + 1/2 + 1/4 + ..::

    >>> gseq(0.5) >> fold(operator.add) >> item[:5]
    [1, 1.5, 1.75, 1.875, 1.9375]

Random Walk in 2D
-----------------
Generate an infinite stream of coordinates representing the position of
a random walker in 2D::

    from random import choice
    vectoradd = lambda u,v: zip(u, v) >> apply(operator.add) >> list
    directions = [[1,0], [0,1], [-1,0], [0,-1]]
    rw = lambda: repeatcall(choice, directions) >> fold(vectoradd, [0, 0])

Calling choice repeatedly yields the series of unit vectors representing the
directions that the walker takes, then these vectors are gradually added to get
a series of coordinates.

To instantiate a random-walk, and get the first 10 coordinates::

    walk = rw()
    walk >> item[:10]

Question: what is the farthest point that the walker wanders upto the first
return to the origin? (Note that he might never return at all!)::

    vectorlen = lambda v: v >> map(lambda x: x**2) >> sum
    rw() >> drop(1) >> takewhile(lambda v: v != [0, 0]) >> maximum(key=vectorlen)

The first coordinate [0, 0], which is the origin, needs to be dropped otherwise
takewhile will truncate immediately.

We can also probe into the walker's chosen path::

    probe = Stream()
    rw() >> drop(1) >> takewhile(lambda v: v != [0, 0]) >> tee(probe) >> maximum(key=vectorlen)

Now you can see his exact coordinates, for example the first 10 are::

	probe >> item[:10]
"""

__version__ = '0.6.1'
__author__ = 'Anh Hai Trinh'
__email__ = 'moc.liamg@hnirt.iah.hna:otliam'[::-1]

import __builtin__
import collections
import itertools
import operator
import re
import string

from operator import itemgetter, attrgetter, methodcaller


#_____________________________________________________________________
#
# Base class for Stream
#_____________________________________________________________________

class BrokenPipe(Exception): pass


class Stream(collections.Iterator):
	"""A class representing both a stream and a filter.

	The outgoing stream is represented by the attribute 'iterator'.

	The filter is represented by  the method __call__(inpipe), which
	combines self's iterator with inpipe's, returning a new iterator
	representing a new outgoing stream.
	
	A Stream subclass will usually implement __call__, unless it is an
	accumulator and will not return a Stream, in which case it needs to
	implement __pipe__.  The default piping mechanism of Stream is appending
	to the end of the its input (which had better terminate!).

	>>> [1, 2, 3] >> Stream('foo') >> Stream('bar') >> list
	[1, 2, 3, 'f', 'o', 'o', 'b', 'a', 'r']
	"""
	__slots__ = 'iterator'

	def __init__(self, iterable=None):
		"""Make a stream object from an interable"""
		self.iterator = iter(iterable if iterable else [])

	def __iter__(self):
		return self.iterator

	def next(self):
		return next(self.iterator)

	def __call__(self, inpipe):
		"""Append to the end of inpipe (it had better terminate!)."""
		return itertools.chain(inpipe, self.iterator)

	def __pipe__(self, inpipe):
		self.iterator = self.__call__(inpipe)
		return self

	@staticmethod
	def pipe(inpipe, outpipe):
		if hasattr(outpipe, '__pipe__'):
			return outpipe.__pipe__(iter(inpipe))
		elif hasattr(outpipe, '__call__'):
			if hasattr(outpipe, '__name__') and outpipe.__name__ == 'list':
				## For some reason `list` doesn't believe that inpipe is an iterator
				return outpipe(iter(inpipe))
			else:
				return outpipe(inpipe)
		else:
			raise BrokenPipe('No connection mechanism defined')

	def __rshift__(self, outpipe):
		return Stream.pipe(self, outpipe)

	def __rrshift__(self, inpipe):
		return Stream.pipe(inpipe, self)

	def __extend__(self, inpipe):
		"""
		Similar to __pipe__, except for the fact that both
		self and inpipe must be Stream instances, in which case
		inpipe.iterator is modified in place.
		"""
		inpipe.iterator = self.__call__(inpipe.iterator)
		return inpipe

	@staticmethod
	def extend(inpipe, outpipe):
		if hasattr(outpipe, '__extend__'):
			return outpipe.__extend__(inpipe)

	def __lshift__(self, outpipe):
		return Stream.extend(self, outpipe)

	def __len__(self):				### this will force all evaluation
		"""
		>>> Stream(range(20)) >> len
		20
		"""
		return len(list(self.iterator))

	def __repr__(self):
		return 'Stream(%s)' % repr(self.iterator)


#_______________________________________________________________________
#
# Filtering streams by element indices
#_______________________________________________________________________

class take(Stream):
	"""Return a Stream of n items taken from the input stream.
	
	>>> seq(1, 2) >> take(10)
	Stream([1, 3, 5, 7, 9, 11, 13, 15, 17, 19])
	"""
	__slots__ = 'items', 'n'

	def __init__(self, n):
		super(take, self).__init__()
		self.n = n
		self.items = []

	def __call__(self, inpipe):
		self.items =  [next(inpipe) for _ in xrange(self.n)]
		return self.items

	def __repr__(self):
		return 'Stream(%s)' % repr(self.items)


negative = lambda x: x and x<0		### since None < 0 == True


class itemtaker(Stream):
	"""
	Implement Python slice notation for take. Return a list.

	>>> a = itertools.count()
	>>> a >> item[:10:2]
	[0, 2, 4, 6, 8]
	>>> a >> item[:5]
	[10, 11, 12, 13, 14]
	>>> xrange(20) >> item[-5]
	15
	>>> xrange(20) >> item[-1]
	19
	>>> xrange(20) >> item[::-2]
	[19, 17, 15, 13, 11, 9, 7, 5, 3, 1]
	"""
	__slots__ = 'slice', 'get1'

	def __init__(self, slice=None, get1=False):
		self.slice = slice
		self.get1 = get1

	@classmethod
	def __getitem__(cls, sliceobj):
		if type(sliceobj) is type(1):
			getter = cls(slice(sliceobj), True)
		elif type(sliceobj) is type(slice(1)):
			getter = cls(sliceobj, False)
		else:
			raise TypeError('index must be an integer or a slice')
		return getter

	def __pipe__(self, inpipe):
		if self.get1:
			## just one item is needed
			i = self.slice.stop
			if i > 0:
				# throw away i values
				collections.deque(itertools.islice(inpipe, i), maxlen=0)
				return next(inpipe)
			else:
				# keep the last -i items
				# since we don't know beforehand when the stream stops
				n = -i if i else 1
				items = collections.deque(itertools.islice(inpipe, None), maxlen=n)
				return items[-n]
		else:
			## a list is needed
			if negative(self.slice.stop) or negative(self.slice.start) \
				or not (self.slice.start or self.slice.stop) \
				or (not self.slice.start and negative(self.slice.step)) \
				or (not self.slice.stop and not negative(self.slice.step)):
				# force all evaluation
				items = list(inpipe)
			else:
				# force some evaluation
				if negative(self.slice.step):
					stop = self.slice.start
				else:
					stop = self.slice.stop
				try:
					items = [next(inpipe) for _ in xrange(stop)]
				except StopIteration:
					pass
			return items[self.slice]

	def __repr__(self):
		return '<itemtaker at %s>' % hex(id(self))

item = itemtaker()


class takei(Stream):
	"""Select elements of the incoming stream by a stream of indexes.
	>>> seq() >> takei([0, 4, 7]) >> list
	[0, 4, 7]

	>>> seq() >> takei(xrange(2,43,4)) >> list
	[2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42]

	>>> xrange(15) >> takei([3, -2, 7, 7]) >> list
	[3, 7]
	"""
	__slots__ = 'indexiter'

	def __init__(self, indices):
		"""indexes should be non-negative integers in monotonically
		increasing order (bad values won't yield)
		"""
		super(takei, self).__init__()
		self.indexiter = iter(indices)

	def __call__(self, inpipe):
		def genfunc():
			old_idx = -1
			idx = next(self.indexiter)			# next value to yield
			counter = seq()
			while 1:
				c = next(counter)
				elem = next(inpipe)
				while idx <= old_idx:			# ignore bad values
					idx = next(self.indexiter)
				if c == idx:
					yield elem
					old_idx = idx
					idx = next(self.indexiter)
		return genfunc()


class drop(Stream):
	"""Drop the first n elements of the incoming stream

	>>> seq(0, 2) >> drop(1) >> take(5)
	Stream([2, 4, 6, 8, 10])
	"""
	__slots__ = 'n'
	
	def __init__(self, n):
		"""n: the number of elements to be dropped"""
		super(drop, self).__init__()
		self.n = n

	def __call__(self, inpipe):
		collections.deque(itertools.islice(inpipe, self.n), maxlen=0)
		return inpipe


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
		super(dropi, self).__init__()
		self.indexiter = iter(indices)

	def __call__(self, inpipe):
		def genfunc():
			counter = seq()
			def try_next_idx():
				## so that the stream keeps going 
				## after the discard iterator is exhausted
				try:
					return next(self.indexiter), False
				except StopIteration:		
					return -1, True
			old_idx = -1
			idx, exhausted = try_next_idx()				# next value to discard
			while 1:
				c =next(counter)
				elem = next(inpipe)
				while not exhausted and idx <= old_idx:	# ignore bad values
					idx, exhausted = try_next_idx()	
				if c != idx:
					yield elem
				elif not exhausted:
					old_idx = idx
					idx, exhausted = try_next_idx()
		return genfunc()


#_______________________________________________________________________
#
# Filtering streams with functions and higher-order ones
#_______________________________________________________________________


class Filter(Stream):
	"""Base class for stream filter based on a function"""

	__slots__ = 'function'

	def __init__(self, function):
		super(Filter, self).__init__()
		self.function = function
	
	def __call__(self, inpipe):
		return self.function(inpipe)


class apply(Filter):
	"""Invoke a function using each stream element as a list of arguments, 
	a la itertools.starmap.
	"""
	def __call__(self, inpipe):
		return itertools.starmap(self.function, inpipe)


class map(Filter):
	def __call__(self, inpipe):
		return itertools.imap(self.function, inpipe)


class itemcutter(map):
	"""Call the method __getitem__ on the input stream using slice notation.

	>>> [range(10), range(10, 20)] >> cut[::2] >> list
	[[0, 2, 4, 6, 8], [10, 12, 14, 16, 18]]
	"""

	def __init__(self, *args):
		super(itemcutter, self).__init__( methodcaller('__getitem__', *args) )

	@classmethod
	def __getitem__(cls, args):
		return cls(args)

	def __repr__(self):
		return '<itemcutter at %s>' % hex(id(self))

cut = itemcutter()


class filter(Filter):
	"""
	>>> even = lambda x: x%2 == 0
	>>> xrange(1, 40, 3) >> filter(even) >> list
	[4, 10, 16, 22, 28, 34]
	"""
	def __call__(self, inpipe):
		return itertools.ifilter(self.function, inpipe)


class takewhile(Filter):
	def __call__(self, inpipe):
		return itertools.takewhile(self.function, inpipe)


class dropwhile(Filter):
	def __call__(self, inpipe):
		return itertools.dropwhile(self.function, inpipe)


class fold(Filter):
	"""
	Combines the elements of inpipe by applying a function of two
	argument to a value and each element in turn.  At each step,
	the value is set to the value returned by the function, thus it
	is, in effect, an accumulation.
	
	This example calculate partial sums of the series 1+1/2+1/4+...

	>>> gseq(0.5) >> fold(operator.add) >> item[:5]
	[1, 1.5, 1.75, 1.875, 1.9375]
	"""
	def __init__(self, function, initval=None):
		super(fold, self).__init__(function)
		self.initval = initval

	def __call__(self, inpipe):
		def genfunc():
			if self.initval:
				accumulated = self.initval
			else:
				accumulated = next(inpipe)
			while 1:
				yield accumulated
				val = next(inpipe)
				accumulated = self.function(accumulated, val)
		return genfunc()


#_____________________________________________________________________
#
# Combining streams
#_____________________________________________________________________


class prepend(Stream):
	"""Prepend at the beginning of a stream.

	>>> seq(7, 7) >> prepend(xrange(0, 10, 2)) >> item[:10]
	[0, 2, 4, 6, 8, 7, 14, 21, 28, 35]
	"""
	def __call__(self, inpipe):
		"""Prepend at the beginning of inpipe"""
		return itertools.chain(self.iterator, inpipe)


class tee(Stream):
	"""Make a branch from a stream.

	>>> foo = filter(lambda x: x%3==0)
	>>> bar = seq(0, 2) >> tee(foo)
	>>> bar >> item[:5]
	[0, 2, 4, 6, 8]
	>>> foo >> item[:5]
	[0, 6, 12, 18, 24]
	"""
	__slots__ = 'streamobj',

	def __init__(self, streamobj):
		super(tee, self).__init__()
		self.streamobj = streamobj

	def __pipe__(self, inpipe):
		"""Make a branch of inpipe to pipe to self.streamobj"""
		branch1, branch2 = itertools.tee(iter(inpipe))
		Stream.pipe(branch1, self.streamobj)
		if isinstance(inpipe, Stream):
			inpipe.iterator = branch2
			return inpipe
		else:
			return Stream(branch2)

#_______________________________________________________________________
#
# Filtering nested streams
#_______________________________________________________________________


class flattener(Stream):
	"""Flatten a nested iterable stream of arbitrary depth, ignoring
	basetring.

	>>> (xrange(i) for i in seq(step=3)) >> flatten >> item[:18]
	[0, 1, 2, 0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5, 6, 7, 8]
	"""
	@staticmethod
	def __call__(inpipe):
		def flatten():
			## Maintain a LIFO stack of iterators
			stack = []
			i = inpipe
			while True:
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
						break
		return flatten()

	def __repr__(self):
		return '<flattener at %s>' % hex(id(self))

flatten = flattener()


class zipwith(Stream):
	"""
	>>> range(10) >> zipwith(range(10, 20), range(20,30)) >> take(5)
	Stream([(0, 10, 20), (1, 11, 21), (2, 12, 22), (3, 13, 23), (4, 14, 24)])
	"""
	__slots__ = 'iterables'
	def __init__(self, *iterables):
		super(zipwith, self).__init__()
		self.iterables = list(iterables)

	def __call__(self, inpipe):
		return itertools.izip(*([inpipe] + self.iterables))


#_____________________________________________________________________
#
# Useful ultilities
#_____________________________________________________________________


def seq(start=0, step=1):
	"""An arithmetic sequence generator.  Works with any type with + defined.

	>>> seq(1, 0.25) >> item[:10]
	[1, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5, 2.75, 3.0, 3.25]
	"""
	def seq(a, d):
		while 1:
			yield a
			a += d
	return seq(start, step)

def gseq(ratio, initval=1):
	"""A geometric sequence generator.  Works with any type with * defined.

	>>> from decimal import Decimal
	>>> gseq(Decimal('.2')) >> item[:4]
	[1, Decimal('0.2'), Decimal('0.04'), Decimal('0.008')]
	"""
	while 1:
		yield initval
		initval *= ratio

def repeatcall(func, *args):
	"""Repeatedly call func(*args) and yield the result.Useful when
	func(*args) returns different results, esp. randomly.
	"""
	return itertools.starmap(func, itertools.repeat(args))

def chaincall(func, initval):
	"""Yield func(initval), func(func(initval)), etc.
	
	>>> chaincall(lambda x: 3*x, 2) >> take(10)
	Stream([2, 6, 18, 54, 162, 486, 1458, 4374, 13122, 39366])
	"""
	x = initval
	while 1:
		yield x
		x = func(x)

def splitter(regex, maxsplit=0):
	"""
	Curried version of re.split.
	
	>>> ['12.3:7', '14.2:5'] >> map(splitter(':|\.')) >> list
	[['12', '3', '7'], ['14', '2', '5']]
	"""
	return lambda s: re.split(regex, s, maxsplit)

def maximum(key):
	"""
	Curried version of the built-in max.
	
	>>> Stream([3, 5, 28, 42, 7]) >> maximum(lambda x: x%28) 
	42
	"""
	return lambda s: max(s, key=key)

def minimum(key):
	"""
	Curried version of the built-in min.
	
	>>> Stream([[13, 52], [28, 35], [42, 6]]) >> minimum(lambda v: v[0] + v[1]) 
	[42, 6]
	"""
	return lambda s: min(s, key=key)

def reduce(function, initval=None):
	"""
	Curried version of the built-in reduce.
	
	>>> reduce(lambda x, y: x+y)( [1, 2, 3, 4, 5] )
	15
	"""
	if initval is None:
		return lambda s: __builtin__.reduce(function, s)
	else:
		return lambda s: __builtin__.reduce(function, s, initval)


if __name__ == "__main__":
	import doctest
	doctest.testmod()
