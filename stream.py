"""Lazily-evaluated stream with pipelining via the '>>' operator.

Streams are generalized iterators with a pipelining mechanism to enable
data-flow programming in Python.

A pipeline usually starts with a generator, then passes through a number
of processors.  Multiple streams can be branched and combined.  Finally,
the output is fed to an accumulator, which can be any function of one
iterable argument.

Generators: anything iterable

Processors: drop, dropwhile, takewhile, map, getname, callmethod, cut,
	filter, reduce, apply, generate

Combinators: prepend, takei, dropi, tee, flatten

Accumulators: take, item
	(already in Python: list, sum, max, min, dict, ...)

Utility functions: seq, gseq, repeatcall, chaincall, attr, method

take() and item[] work similarly, except for notation and the fact that
item[] returns a list whereas take() returns a stream which can be further
piped to another accumulator.

Values are computed only when an accumulator forces some or all evaluation
(not when the stream are set up).
"""


__version__ = '0.5'
__author__ = 'Anh Hai Trinh'
__email__ = 'moc.liamg@hnirt.iah.hna:otliam'[::-1]
__all__ = [
	'seq',
	'gseq',
	'repeatcall',
	'chaincall',
	'attr',
	'method',
	'cond',
	'Stream',
	'take',
	'takeall',
	'item',
	'takei',
	'drop',
	'dropi',
	'apply',
	'map',
	'cut',
	'filter',
	'reduce',
	'takewhile',
	'dropwhile',
	'generate',
	'tee',
	'prepend',
	'flatten',
]

import itertools
import collections


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

def gseq(*args):
	"""A geometric sequence generator.  Works with any type with * defined.

	>>> from decimal import Decimal
	>>> gseq(Decimal('.2')) >> item[:4]
	[1, Decimal('0.2'), Decimal('0.04'), Decimal('0.008')]
	"""
	def gseq(a, r):
		while 1:
			yield a
			a *= r
	if len(args) == 1:
		return gseq(1, args[0])
	elif len(args) == 2:
		return gseq(args[0], args[1])
	else:
		raise TypeError('gseq expects 1 or 2 arguments, got %s' % len(args))

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

def attr(*names):
	"""Return a function that gets the named attributes of an object.

	>>> class Foo:
	...		def __init__(self, x, y):
	...			self.x, self.y = x, y
	>>> [Foo(2, 7), Foo(1, 4)] >> map(attr('x', 'x', 'y')) >> item[:]
	[[2, 2, 7], [1, 1, 4]]
	"""
	return lambda obj: [getattr(obj, a) for a in names]


def method(m, *args, **kwargs):
	"""Return a function that calls the method m(*args, **kwargs) of an object.

	>>> ['foo bar 1', 'foo bar 2'] >> map(method('split')) >> list
	[['foo', 'bar', '1'], ['foo', 'bar', '2']]
	"""
	return lambda obj: getattr(obj, m)(*args, **kwargs)


def cond(predicate, consequence, alternative=None):
	"""
	Functional replacement for if-else to use in expressions.

		>>> x = 2
		>>> cond(x % 2 == 0, "even", "odd")
		'even'
		>>> cond(x % 2 == 0, "even", "odd") + '_row'
		'even_row'
	"""
	if predicate:
		return consequence
	else:
		return alternative

#_____________________________________________________________________
#
# Base class for stream processor
#_____________________________________________________________________

class BrokenPipeError(Exception): pass


class Stream(collections.Iterable):
	"""A class representing both a stream and a processor.

	The outgoing stream is represented by the attribute 'iterable'.

	The processor is represented by  the method __call__(inpipe), which
	combines self's iterable with inpipe's, returning a new iterable
	representing a new outgoing stream.
	
	A Stream subclass will usually implement __call__, unless it is an
	accumulator and will not return a Stream, in which case it needs to
	implement __pipe__.  The default piping mechanism of Stream is appending
	to the end of the its input (which had better terminate!).

	>>> [1, 2, 3] >> Stream('foo') >> Stream('bar') >> list
	[1, 2, 3, 'f', 'o', 'o', 'b', 'a', 'r']
	"""
	__slots__ = 'iterable'

	def __init__(self, iterable=[]):
		"""Make a stream object from an iterable"""
		self.iterable= iterable if iterable else []

	def __iter__(self):
		return iter(self.iterable)

	def __call__(self, inpipe):
		"""Append to the end of inpipe(it had better terminate!)."""
		return itertools.chain(inpipe, self.iterable)

	def __pipe__(self, inpipe):
		self.iterable = self.__call__(iter(inpipe))
		return self

	@staticmethod
	def pipe(inpipe, outpipe):
		if hasattr(outpipe, '__pipe__'):
			return outpipe.__pipe__(inpipe)
		elif hasattr(outpipe, '__call__'):
			if outpipe.__name__ == 'list':
				return outpipe(iter(inpipe))	## God knows why this happens ##
			else:
				return outpipe(inpipe)
		else:
			raise BrokenPipeError('No connection mechanism defined')

	def __rshift__(self, outpipe):
		return Stream.pipe(self, outpipe)

	def __rrshift__(self, inpipe):
		return Stream.pipe(inpipe, self)

	def __extend__(self, inpipe):
		"""
		Similar to __pipe__, except for the fact that both
		self and inpipe must be Stream instances, in which case
		inpipe.iterable is modified in place.
		"""
		inpipe.iterable = self.__call__(inpipe.iterable)
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
		return len(list(self.iterable))

	def __repr__(self):
		return 'Stream(%s)' % repr(self.iterable)


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
	__slots__ = 'slice'

	def __init__(self, *args):
		super(take, self).__init__()
		self.slice = slice(*args)

	def __call__(self, inpipe):
		if negative(self.slice.stop) or negative(self.slice.start) \
		or not (self.slice.start or self.slice.stop) \
		or (not self.slice.start and negative(self.slice.step)) \
		or (not self.slice.stop and not negative(self.slice.step)):
			self.iterable = list(inpipe)		## force all evaluation 	##
		else:							## force some evaluation ##
			if negative(self.slice.step):
				stop = self.slice.start
			else:
				stop = self.slice.stop
			try:
				self.iterable =  [next(inpipe) for _ in xrange(stop)]
			except StopIteration:
				pass
		self.iterable = self.iterable[self.slice]
		return self.iterable

	def __repr__(self):
		return 'Stream(%s)' % repr(self.iterable)

takeall = take(None)


class itemgetter(take):
	"""
	Implement Python slice notation for take. Return a list.

	>>> a = itertools.count()
	>>> a >> item[:10:2]
	[0, 2, 4, 6, 8]
	>>> a >> item[:5]
	[10, 11, 12, 13, 14]
	>>> xrange(20) >> item[-2]
	18
	>>> xrange(20) >> item[::-2]
	[19, 17, 15, 13, 11, 9, 7, 5, 3, 1]
	"""
	__slots__ = 'get1'

	def __init__(self):
		self.get1 = False

	@classmethod
	def __getitem__(cls, sliceobj):
		getter = cls()
		if type(sliceobj) is type(1):
			getter.get1 = True
			if sliceobj == -1:
				sliceobj = None
			else:
				sliceobj += 1
			getter.slice = slice(sliceobj)
		else:
			getter.slice = sliceobj
		return getter

	def __pipe__(self, inpipe):
		super(itemgetter, self).__call__(iter(inpipe))
		if self.get1:
			return self.iterable[-1]
		else:
			return self.iterable

	def __repr__(self):
		return '<itemgetter at %s>' % hex(id(self))

item = itemgetter()

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
			idx, exhausted = try_next_idx()			# next value to discard
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
# Functional processing
#_______________________________________________________________________


class FunctionFilter(Stream):
	"""Base class for stream filter based on a function"""

	__slots__ = 'function'

	def __init__(self, function):
		super(FunctionFilter, self).__init__()
		self.function = function


class apply(FunctionFilter):
	"""Invoke a function using each stream element as a list of arguments, 
	a la itertools.starmap.
	"""
	def __call__(self, inpipe):
		return itertools.starmap(self.function, inpipe)


class map(FunctionFilter):
	def __call__(self, inpipe):
		return itertools.imap(self.function, inpipe)


class itemcutter(map):
	"""Map the method __getitem__ on the input stream using slice notation.

	>>> [range(10), range(10, 20)] >> cut[::2] >> list
	[[0, 2, 4, 6, 8], [10, 12, 14, 16, 18]]
	"""

	def __init__(self, *args):
		super(itemcutter, self).__init__( method('__getitem__', *args) )

	@classmethod
	def __getitem__(cls, args):
		return cls(args)

	def __repr__(self):
		return '<cutter at %s>' % hex(id(self))

cut = itemcutter()


class filter(FunctionFilter):
	"""
	>>> even = lambda x: x%2 == 0
	>>> xrange(1, 40, 3) >> filter(even) >> list
	[4, 10, 16, 22, 28, 34]
	"""
	def __call__(self, inpipe):
		return itertools.ifilter(self.function, inpipe)


class takewhile(FunctionFilter):
	def __call__(self, inpipe):
		return itertools.takewhile(self.function, inpipe)


class dropwhile(FunctionFilter):
	def __call__(self, inpipe):
		return itertools.dropwhile(self.function, inpipe)


class reduce(FunctionFilter):
	"""Haskell's scanl.

	>>> from operator import add
	>>> gseq(0.5) >> reduce(add) >> item[:5]
	[1, 1.5, 1.75, 1.875, 1.9375]
	"""
	def __init__(self, function, initval=None):
		super(reduce, self).__init__(function)
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


class generate(Stream):
	"""Eval an generator expression where {0} indicates the input stream.

	>>> range(10) >> generate("x if x % 2 else 'x' for x in {0}") >> list
	['x', 1, 'x', 3, 'x', 5, 'x', 7, 'x', 9]
	"""
	__slots__ = 'expr'

	def __init__(self, expr):
		super(generate, self).__init__()
		self.expr = expr

	def __call__(self, inpipe):
		return eval( '(' + self.expr.format('inpipe') + ')' )


#_____________________________________________________________________
#
# Stream combinators
#_____________________________________________________________________


class prepend(Stream):
	"""Prepend at the beginning of a stream.

	>>> seq(7, 7) >> prepend(xrange(0, 10, 2)) >> item[:10]
	[0, 2, 4, 6, 8, 7, 14, 21, 28, 35]
	"""
	def __call__(self, inpipe):
		"""Prepend at the beginning of inpipe"""
		return itertools.chain(self.iterable, inpipe)


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
			inpipe.iterable = branch2
			return inpipe
		else:
			return Stream(branch2)

#_______________________________________________________________________
#
# Nested streams processing
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


if __name__ == "__main__":
	import doctest
	doctest.testmod()
