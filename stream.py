"""Lazily-evaluated stream with pipelining via the '>>' operator.

Overview
========

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
	+ by index:  take, drop, takei, dropi
	+ by condition:  filter, takewhile, dropwhile
	+ by transformation:  apply, map, fold
	+ by combining streams:  prepend, tee
	+ for special purpose:  chop, cut, flatten

**Accumulators**:  item, maximum, minimum, reduce
	+ from Python:  list, sum, dict, max, min ...

Values are computed only when an accumulator forces some or all evaluation
(not when the stream are set up).


Parallelization
---------------

When a producer is doing blocking I/O, it is possible to use a ThreadedFeeder
or ForkedFeeder to improve performance.  The feeder will start a thread or a
process to run the producer and feed generated items back to the pipeline, thus
minimizing the time that the whole pipeline has to wait when the producer is
blocking in system calls.

If the order of processing does not matter, an ThreadPool or ProcessPool
can be used.  They both utilize a number of workers in other theads
or processes to work on items pulled from the piped input.  It is also
possible to submit jobs to a thread/process pool directly.  An Executor
can perform fine-grained, concurrent job control for a thread/process pool.

Multiple streams can be piped to a single PCollector or QCollector, which
will gather generated items whenever they are avaiable.  PCollectors
can collect from ForkedFeeder's or ProcessPool's (via system pipes) and
QCollector's can collect from ThreadedFeeder's and ThreadPool's (via queues).
PSorter and QSorter are also collectors, but given multiples sorted input
streams (low to high), a Sorter will output items in sorted order.

Using multiples Feeder's and Collector's, one can implement many parallel
processing patterns:  fan-in, fan-out, many-to-many map-reduce, etc.


Articles
========

Articles written about this module by the author can be retrieved from
<http://blog.onideas.ws/project:stream.py>.


Examples
========

Slicing iterators
-----------------
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
separated by ' ', ':' or '.', then save as a list::

    import re
    s = open('file') \
      >> filter(re.compile(regex).search) \
      >> map(re.compile(' |:|\.').split) \
      >> map(itemgetter(3)) \
      >> list

Partial sums
------------
Compute the first few partial sums of the geometric series 1 + 1/2 + 1/4 + ..::

    >>> gseq(0.5) >> fold(operator.add) >> item[:5]
    [1, 1.5, 1.75, 1.875, 1.9375]
"""

__version__ = '0.8'


import __builtin__
import copy
import collections
import heapq
import itertools
import multiprocessing
import multiprocessing.queues
import operator
import Queue
import re
import select
import sys
import threading
import time

from operator import itemgetter, attrgetter

try:
	from operator import methodcaller
except ImportError:
	def methodcaller(method, *args, **kwargs):
		return lambda o: o.method(*args, **kwargs)

zip = itertools.izip


#_____________________________________________________________________
# Base class


class BrokenPipe(Exception):
	pass


class Stream(collections.Iterable):
	"""A class representing both a stream and a filter.

	The outgoing stream is represented by the attribute 'iterator'.

	The filter is represented by the method __call__(inpipe), which
	combines self's iterator with inpipe's, returning a new iterator
	representing a new outgoing stream.
	
	A Stream subclass will usually implement __call__, unless it is an
	accumulator and will not return a Stream, in which case it needs to
	implement __pipe__.  The default piping mechanism of Stream is appending
	to the end of the its input (which had better terminate!).

	>>> [1, 2, 3] >> Stream('foo') >> Stream('bar') >> list
	[1, 2, 3, 'f', 'o', 'o', 'b', 'a', 'r']
	"""
	def __init__(self, iterable=None):
		"""Make a stream object from an interable."""
		self.iterator = iter(iterable if iterable else [])

	def __iter__(self):
		return self.iterator

	def __call__(self, inpipe):
		return itertools.chain(inpipe, self.iterator)

	def __pipe__(self, inpipe):
		self.iterator = self.__call__(iter(inpipe))
		return self

	@staticmethod
	def pipe(inpipe, outpipe):
		"""Connect inpipe and outpipe.  If outpipe is not a Stream instance,
		it should be an function callable on an iterable.
		"""
		if hasattr(outpipe, '__pipe__'):
			return outpipe.__pipe__(inpipe)
		elif hasattr(outpipe, '__call__'):
			return outpipe(inpipe)
		else:
			raise BrokenPipe('No connection mechanism defined')

	def __rshift__(self, outpipe):
		return Stream.pipe(self, outpipe)

	def __rrshift__(self, inpipe):
		return Stream.pipe(inpipe, self)

	def extend(self, inpipe):
		"""Similar to pipe(), except that extend() is an instance method,
		and inpipe must be a Stream, in which case inpipe.iterator is
		modified in-place.
		"""
		inpipe.iterator = self.__call__(inpipe.iterator)
		return inpipe

	def __repr__(self):
		return 'Stream(%s)' % repr(self.iterator)


#_______________________________________________________________________
# Filtering streams by element indices


class take(Stream):
	"""Take the firts n items of the input stream, return a Stream.
	
	>>> seq(1, 2) >> take(10)
	Stream([1, 3, 5, 7, 9, 11, 13, 15, 17, 19])
	"""
	def __init__(self, n):
		"""n: the number of elements to be taken"""
		super(take, self).__init__()
		self.n = n
		self.items = []

	def __call__(self, inpipe):
		self.items =  list(itertools.islice(inpipe, self.n))
		return iter(self.items)

	def __repr__(self):
		return 'Stream(%s)' % repr(self.items)


negative = lambda x: x and x<0		### since None < 0 == True


class itemtaker(Stream):
	"""
	Slice the input stream, return a list.

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
				items = [i for i in inpipe]
			else:
				# force some evaluation
				if negative(self.slice.step):
					stop = self.slice.start
				else:
					stop = self.slice.stop
				items = list(itertools.islice(inpipe, stop))
			return items[self.slice]

	def __repr__(self):
		return '<itemtaker at %s>' % hex(id(self))

item = itemtaker()


class takei(Stream):
	"""Select elements of the input stream by indices.

	>>> seq() >> takei(xrange(2, 43, 4)) >> list
	[2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42]
	"""
	def __init__(self, indices):
		"""indices: an iterable of indices to be taken, should yield
		non-negative integers in monotonically increasing order
		"""
		super(takei, self).__init__()
		self.indexiter = iter(indices)

	def __call__(self, inpipe):
		def itaker():
			old_idx = -1
			idx = next(self.indexiter)                # next value to yield
			counter = seq()
			while 1:
				c = next(counter)
				elem = next(inpipe)
				while idx <= old_idx:               # ignore bad values
					idx = next(self.indexiter)
				if c == idx:
					yield elem
					old_idx = idx
					idx = next(self.indexiter)
		return itaker()


class drop(Stream):
	"""Drop the first n elements of the input stream.

	>>> seq(0, 2) >> drop(1) >> take(5)
	Stream([2, 4, 6, 8, 10])
	"""
	def __init__(self, n):
		"""n: the number of elements to be dropped"""
		super(drop, self).__init__()
		self.n = n

	def __call__(self, inpipe):
		collections.deque(itertools.islice(inpipe, self.n), maxlen=0)
		return inpipe


class dropi(Stream):
	"""Drop elements of the input stream by indices.

	>>> seq() >> dropi(seq(0,3)) >> item[:10]
	[1, 2, 4, 5, 7, 8, 10, 11, 13, 14]
	"""
	def __init__(self, indices):
		"""indices: an iterable of indices to be dropped, should yield
		non-negative integers in monotonically increasing order
		"""
		super(dropi, self).__init__()
		self.indexiter = iter(indices)

	def __call__(self, inpipe):
		def idropper():
			counter = seq()
			def try_next_idx():
				## so that the stream keeps going 
				## after the discard iterator is exhausted
				try:
					return next(self.indexiter), False
				except StopIteration:		
					return -1, True
			old_idx = -1
			idx, exhausted = try_next_idx()                  # next value to discard
			while 1:
				c = next(counter)
				elem = next(inpipe)
				while not exhausted and idx <= old_idx:    # ignore bad values
					idx, exhausted = try_next_idx()	
				if c != idx:
					yield elem
				elif not exhausted:
					old_idx = idx
					idx, exhausted = try_next_idx()
		return idropper()


#_______________________________________________________________________
# Filtering streams with functions and higher-order ones


class Filter(Stream):
	"""A decorator to turn an iterator-processing function into
	a Stream filter.
	"""
	def __init__(self, function):
		"""function: an iterator-processing function, one that takes an
		iterator and return an iterator
		"""
		super(Filter, self).__init__()
		self.function = function
	
	def __call__(self, inpipe):
		return self.function(inpipe)


class apply(Stream):
	"""Invoke a function using each element of the input stream unpacked as
	its argument list, a la itertools.starmap.
	"""
	def __init__(self, function):
		"""function: to be called with each stream element unpacked as its
		argument list
		"""
		super(apply, self).__init__()
		self.function = function

	def __call__(self, inpipe):
		return itertools.starmap(self.function, inpipe)


class map(Stream):
	"""Invoke a function using each element of the input stream as its only
	argument, a la itertools.imap.
	"""
	def __init__(self, function):
		"""function: to be called with each stream element as its
		only argument
		"""
		super(map, self).__init__()
		self.function = function

	def __call__(self, inpipe):
		return itertools.imap(self.function, inpipe)


class filter(Filter):
	"""Filter the input stream, selecting only values which evaluates to True
	by the given function, a la itertools.ifilter.

	>>> even = lambda x: x%2 == 0
	>>> range(10) >> filter(even) >> list
	[0, 2, 4, 6, 8]
	"""
	def __call__(self, inpipe):
		return itertools.ifilter(self.function, inpipe)


class takewhile(Filter):
	"""Take items from the input stream that come before the first item to
	evaluate to False by the given function, a la itertools.takewhile.
	"""
	def __call__(self, inpipe):
		return itertools.takewhile(self.function, inpipe)


class dropwhile(Filter):
	"""Drop items from the input stream that come before the first item to
	evaluate to False by the given function, a la itertools.dropwhile.
	"""
	def __call__(self, inpipe):
		return itertools.dropwhile(self.function, inpipe)


class fold(Filter):
	"""Combines the elements of inpipe by applying a function of two argument
	to a value and each element in turn.  At each step, the value is set to
	the value returned by the function, thus it is, in effect, an
	accumulation.
	
	This example calculate partial sums of the series 1 + 1/2 + 1/4 +...

	>>> gseq(0.5) >> fold(operator.add) >> item[:5]
	[1, 1.5, 1.75, 1.875, 1.9375]
	"""
	def __init__(self, function, initval=None):
		super(fold, self).__init__(function)
		self.initval = initval

	def __call__(self, inpipe):
		def folder():
			if self.initval:
				accumulated = self.initval
			else:
				accumulated = next(inpipe)
			while 1:
				yield accumulated
				val = next(inpipe)
				accumulated = self.function(accumulated, val)
		return folder()


#_____________________________________________________________________
# Special purpose stream filters


class chop(Stream):
	"""Chop the input stream into segments of length n.

	>>> range(10) >> chop(3) >> list
	[[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
	"""
	def __init__(self, n):
		"""n: the length of the segments"""
		super(chop, self).__init__()
		self.n = n

	def __call__(self, inpipe):
		def chopper():
			while 1:
				s = inpipe >> item[:self.n]
				if s:
					yield s
				else:
					break
		return chopper()


class itemcutter(map):
	"""Slice each element of the input stream.

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


class flattener(Stream):
	"""Flatten a nested stream of arbitrary depth.

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


#_______________________________________________________________________
# Combining multiple streams


class prepend(Stream):
	"""Inject values at the beginning of the input stream.

	>>> seq(7, 7) >> prepend(xrange(0, 10, 2)) >> item[:10]
	[0, 2, 4, 6, 8, 7, 14, 21, 28, 35]
	"""
	def __call__(self, inpipe):
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


#_____________________________________________________________________
# Threaded/forked feeder


def _iterqueue(queue):
	"""Turn a either a threading.Queue or a multiprocessing.queues.SimpleQueue
	into an thread-safe iterator which will exhaust when StopIteration is put
	into it.
	"""
	while 1:
		item = queue.get()
		if item is StopIteration:
			# Re-broadcast, in case there is another thread blocking on
			# queue.get().  That thread will receive StopIteration and
			# re-broadcast to the next one in line.
			queue.put(StopIteration)
			break
		else:
			yield item


class ThreadedFeeder(collections.Iterable):
	def __init__(self, generator, *args, **kwargs):
		"""Create a feeder that start the given generator with
		*args and **kwargs in a separate thread.  The feeder will
		act as an eagerly evaluating proxy of the generator.
		
		The feeder can then be iter()'ed over by other threads.
		
		This should improve performance when the generator often
		blocks in system calls.
		"""
		self.output_queue = Queue.Queue()
		def feeder():
			i = generator(*args, **kwargs)
			while 1:
				try:
					self.output_queue.put(next(i))
				except StopIteration:
					self.output_queue.put(StopIteration)
					break
		self.thread = threading.Thread(target=feeder)
		self.thread.start()
	
	def __iter__(self):
		return _iterqueue(self.output_queue)

	def __repr__(self):
		return '<ThreadedFeeder at %s>' % hex(id(self))


class ForkedFeeder(collections.Iterable):
	def __init__(self, generator, *args, **kwargs):
		"""Create a feeder that start the given generator with
		*args and **kwargs in a child process. The feeder will
		act as an eagerly evaluating proxy of the generator.

		The feeder can then be iter()'ed over by other processes.

		This should improve performance when the generator often
		blocks in system calls.  Note that serialization could
		be costly.
		"""
		self.outpipe, inpipe = multiprocessing.Pipe(duplex=False)
		def feed():
			i = generator(*args, **kwargs)
			while 1:
				try:
					inpipe.send(next(i))
				except StopIteration:
					inpipe.send(StopIteration)
					break
		self.process = multiprocessing.Process(target=feed)
		self.process.start()
	
	def __iter__(self):
		def iterrecv(pipe):
			while 1:
				try:
					item = pipe.recv()
				except EOFError:
					break
				else:
					if item is StopIteration:
						break
					else:
						yield item
		return iterrecv(self.outpipe)
	
	def __repr__(self):
		return '<ForkedFeeder at %s>' % hex(id(self))


#_____________________________________________________________________
# Asynchronous stream filters using a pool of threads or processes

_nCPU = multiprocessing.cpu_count()	

class ThreadPool(Stream):
	"""Work on the input stream asynchronously using a pool of threads.
	
	>>> results = range(10) >> ThreadPool(map(lambda x: x*x)) >> set
	>>> results == set([0, 1, 4, 9, 16, 25, 36, 49, 64, 81])
	True
	
	If an input value causes an Exception to be raised, the tuple
	(value, exception) is put into the pool's `failqueue`. The attribute
	`failure` is a thead-safe iterator over the failqueue.  (The pool object
	is still an iterable over the output values as before)

	An alternate way to use ThreadPool is to instantiate it, then submit
	jobs to its `inqueue` concurrently, using StopIteration to mark the end.
	This would be necessary if failed jobs need to be resubmitted.
	
	See also: Executor
	"""
	def __init__(self, function, poolsize=_nCPU, args=[], kwargs={}):
		"""function: an iterator-processing function, one that takes an
		iterator and return an iterator
		"""
		super(ThreadPool, self).__init__()
		self.function = function
		self.inqueue = Queue.Queue()
		self.outqueue = Queue.Queue()
		self.failqueue = Queue.Queue()
		self.failure = Stream(_iterqueue(self.failqueue))
		self.finished = False
		def work():
			input, dupinput = itertools.tee(_iterqueue(self.inqueue))
			output = self.function(input, *args, **kwargs)
			while 1:
				try:
					self.outqueue.put(next(output))
					next(dupinput)
				except StopIteration:
					break
				except Exception as e:
					self.failqueue.put((next(dupinput), e))
		self.worker_threads = []
		for _ in range(poolsize):
			t = threading.Thread(target=work)
			self.worker_threads.append(t)
			t.start()
		def cleanup():
			# Wait for all workers to finish, then signal the end of outqueue and failqueue.
			for t in self.worker_threads:
				t.join()
			self.outqueue.put(StopIteration)
			self.failqueue.put(StopIteration)
			self.finished = True
		self.cleaner_thread = threading.Thread(target=cleanup)
		self.cleaner_thread.start()
	
	def __iter__(self):
		return _iterqueue(self.outqueue)
	
	def __call__(self, inpipe):
		if self.finished:
			raise BrokenPipe('All workers are dead, refusing to summit jobs. '
					'Use another Pool.')
		def feed():
			for item in inpipe:
				self.inqueue.put(item)
			self.inqueue.put(StopIteration)
		self.feeder_thread = threading.Thread(target=feed)
		self.feeder_thread.start()
	
	def __repr__(self):
		return '<ThreadPool at %s>' % hex(id(self))


class ProcessPool(Stream):
	"""Work on the input stream asynchronously using a pool of processes.
	
	>>> results = range(10) >> ProcessPool(map(lambda x: x*x)) >> set
	>>> results == set([0, 1, 4, 9, 16, 25, 36, 49, 64, 81])
	True
	
	If an input value causes an Exception to be raised, the tuple
	(value, exception) is put into the pool's `failqueue`. The attribute
	`failure` is a thead-safe iterator over the failqueue.  (The pool object
	is still an iterable over the output values as before)

	An alternate way to use ProcessPool is to instantiate it, then submit
	jobs to its `inqueue` concurrently, using StopIteration to mark the end.
	This would be necessary if failed jobs need to be resubmitted.
	
	See also: Executor
	"""
	def __init__(self, function, poolsize=_nCPU, args=[], kwargs={}):
		"""function: an iterator-processing function, one that takes an
		iterator and return an iterator
		"""
		super(ProcessPool, self).__init__()
		self.function = function
		self.poolsize = poolsize
		self.inqueue = multiprocessing.queues.SimpleQueue()
		self.outqueue = multiprocessing.queues.SimpleQueue()
		self.failqueue = multiprocessing.queues.SimpleQueue()
		self.failure = Stream(_iterqueue(self.failqueue))
		self.finished = False
		def work():
			input, dupinput = itertools.tee(_iterqueue(self.inqueue))
			output = self.function(input, *args, **kwargs)
			while 1:
				try:
					self.outqueue.put(next(output))
					next(dupinput)
				except StopIteration:
					break
				except Exception as e:
					self.failqueue.put((next(dupinput), e))
		self.worker_processes = []
		for _ in range(self.poolsize):
			p = multiprocessing.Process(target=work)
			self.worker_processes.append(p)
			p.start()
		def cleanup():
			# Wait for all workers to finish, then signal the end of outqueue and failqueue.
			for p in self.worker_processes:
				p.join()
			self.outqueue.put(StopIteration)
			self.failqueue.put(StopIteration)
			self.finished = True
		self.cleaner_thread = threading.Thread(target=cleanup)
		self.cleaner_thread.start()
	
	def __iter__(self):
		return _iterqueue(self.outqueue)
	
	def __call__(self, inpipe):
		if self.finished:
			raise BrokenPipe('All workers are dead, refusing to summit jobs. '
					'Use another Pool.')
		def feed():
			for item in inpipe:
				self.inqueue.put(item)
			self.inqueue.put(StopIteration)
		self.feeder_thread = threading.Thread(target=feed)
		self.feeder_thread.start()
	
	def __repr__(self):
		return '<ProcessPool at %s>' % hex(id(self))


class Executor(object):
	"""
	Provide a fine-grained level of control over a ThreadPool or ProcessPool.
	
	>>> executor = Executor(ProcessPool, map(lambda x: x*x))
	>>> job_ids = executor.submit(*range(10))
	>>> foo_id = executor.submit('foo')
	>>> executor.finish()
	>>> set(executor.result) == set([0, 1, 4, 9, 16, 25, 36, 49, 64, 81])
	True
	>>> list(executor.failure)
	[('foo', TypeError("can't multiply sequence by non-int of type 'str'",))]
	"""
	def __init__(self, poolclass, function, poolsize=_nCPU, args=[], kwargs={}):
		def process_job_id(input):
			input, dupinput = itertools.tee(input)
			id = iter(dupinput >> cut[0])
			input = iter(input >> cut[1])
			output = function(input)
			for item in output:
				yield next(id), item
		self.pool = poolclass(process_job_id,
				          poolsize=poolsize,
					    args=args,
					    kwargs=kwargs)
		self.jobcount = 0
		self.status = []
		self.waitqueue = Queue.Queue()
		if poolclass is ProcessPool:
			self.resultqueue = multiprocessing.queues.SimpleQueue()
			self.failqueue = multiprocessing.queues.SimpleQueue()
		else:
			self.resultqueue = Queue.Queue()
			self.failqueue = Queue.Queue()
		self.result = Stream(_iterqueue(self.resultqueue))
		self.failure = Stream(_iterqueue(self.failqueue))
		
		self.statupdate_lock = threading.Lock()
		## Acquired by trackers to update job statuses.

		self.sema = threading.BoundedSemaphore(poolsize)
		## Used to throttle transfer from waitqueue to pool.inqueue,
		## acquired by input_feeder, released by trackers.
		
		def feed_input():
			while 1:
				id, item = self.waitqueue.get()
				if item is StopIteration:
					break
				else:
					self.sema.acquire()
					with self.statupdate_lock:
						if self.status[id] == 'SUBMITTED':
							self.pool.inqueue.put((id, item))
							self.status[id] = 'RUNNING'
						else:
							self.sema.release()
			self.pool.inqueue.put(StopIteration)
		self.inputfeeder_thread = threading.Thread(target=feed_input)
		self.inputfeeder_thread.start()
		
		def track_result():
			for id, item in self.pool:
				self.sema.release()
				with self.statupdate_lock:
					self.status[id] = 'FINISHED'
				self.resultqueue.put(item)
			self.resultqueue.put(StopIteration)
		self.resulttracker_thread = threading.Thread(target=track_result)
		self.resulttracker_thread.start()
		
		def track_failure():
			for outval, exception in self.pool.failure:
				self.sema.release()
				id, item = outval
				with self.statupdate_lock:
					self.status[id] = 'FAILED'
				self.failqueue.put((item, exception))
			self.failqueue.put(StopIteration)
		self.failuretracker_thread = threading.Thread(target=track_failure)
		self.failuretracker_thread.start()
	
	def submit(self, *items):
		"""Return job ids assigned to the submitted items."""
		with self.statupdate_lock:
			id = self.jobcount
			self.status += ['SUBMITTED'] * len(items)
			self.jobcount += len(items)
		for item in items:
			self.waitqueue.put((id, item))
			id += 1
		if len(items) == 1:
			return id
		else:
			return range(id - len(items), id)
	
	def cancel(self, *ids):
		"""Try to cancel jobs with associated ids.  Return the actual number
		of jobs cancelled.
		"""
		ncancelled = 0
		with self.statupdate_lock:
			for id in ids:
				try:
					if self.status[id] == 'SUBMITTED':
						self.status[id] = 'CANCELLED'
						ncancelled += 1
				except IndexError:
					pass
		return ncancelled
	
	def finish(self):
		"""Indicate that there will be no more job submission."""
		self.waitqueue.put((None, StopIteration))
	
	def shutdown(self):
		"""Shut down the Executor.  Cancel all pending jobs.
		Running workers will stop after finishing their current job items.
		
		This call will block until all workers die.
		"""
		with self.statupdate_lock:
			self.pool.inqueue.put(StopIteration)
			self.waitqueue.put((None, StopIteration))
			_iterqueue(self.waitqueue) >> item[-1]
		self.inputfeeder_thread.join()
		self.resulttracker_thead.join()
		self.failuretracker_thread.join()


#_____________________________________________________________________
# Collectors


class PCollector(Stream):
	"""Collect items from a ForkedFeeder or ProcessPool.
	"""
	def __init__(self):
		self.inpipes = []
		def selrecv():
			while self.inpipes:
				ready, _, _ = select.select(self.inpipes, [], [])
				for inpipe in ready:
					item = inpipe.recv()
					if item is StopIteration:
						del self.inpipes[self.inpipes.index(inpipe)]
					else:
						yield item
		self.iterator = selrecv()
	
	def __pipe__(self, inpipe):
		self.inpipes.append(inpipe.outpipe)
	
	def __repr__(self):
		return '<Collector at %s>' % hex(id(self))


class _PCollector(Stream):
	"""Collect items from a ForkedFeeder or ProcessPool.

	All input pipes are polled individually.  When none is ready, the
	collector sleeps for a fix duration before polling again.
	"""
	def __init__(self, waittime=0.1):
		"""waitime: the duration that the collector sleeps for
		when all input pipes are empty
		"""
		self.inpipes = []
		self.waittime = waittime
		def pollrecv():
			while self.inpipes:
				ready = [p for p in self.inpipes if p.poll()]
				for inpipe in ready:
					item = inpipe.recv()
					if item is StopIteration:
						del self.inpipes[self.inpipes.index(inpipe)]
					else:
						yield item
		self.iterator = pollrecv()
	
	def __pipe__(self, inpipe):
		self.inpipes.append(inpipe.outpipe)
	
	def __repr__(self):
		return '<Collector at %s>' % hex(id(self))

if sys.platform == "win32":
	PCollector = _PCollector


class QCollector(Stream):
	"""Collect items from a ThreadedFeeder or ThreadPool, or anything that
	has an attribute `outqueue` from which we can get().
	
	All input queues are polled individually.  When none is ready, the
	collector sleeps for a fix duration before polling again.
	"""
	def __init__(self, waittime=0.1):
		"""waitime: the duration that the collector sleeps for
		when all input pipes are empty
		"""
		self.inqueues = []
		self.waittime = waittime
		def get():
			while self.inqueues:
				ready = [q for q in self.inqueues if not q.empty()]
				if not ready:
					time.sleep(self.waittime)
				for q in ready:
					item = q.get()
					if item is StopIteration:
						del self.inqueues[self.inqueues.index(q)]
					else:
						yield item
		self.iterator = get()
	
	def __pipe__(self, inpipe):
		self.inqueues.append(inpipe.outqueue)
	
	def __repr__(self):
		return '<QCollector at %s>' % hex(id(self))


class PSorter(Stream):
	"""Merge sorted input (smallest to largest) coming from many
	ForkedFeeder's or ProcessPool's.
	"""
	def __init__(self):
		self.inpipes = []

	def run(self):
		self.inqueues = [Queue.Queue() for _ in xrange(len(self.inpipes))]
		def collect():
			# We make a shallow copy of self.inpipes, as we will be
			# mutating it but still need to refer to the correct queue index
			qindex = copy.copy(self.inpipes).index
			while self.inpipes:
				ready, _, _ = select.select(self.inpipes, [], [])
				for inpipe in ready:
					item = inpipe.recv()
					self.inqueues[qindex(inpipe)].put(item)
					if item is StopIteration:
						del self.inpipes[self.inpipes.index(inpipe)]
		self.collector_thread = threading.Thread(target=collect)
		self.collector_thread.start()
		
		sorter = ThreadedFeeder(heapq.merge, *__builtin__.map(_iterqueue, self.inqueues))
		self.sorter_thread = sorter.thread
		self.iterator = iter(sorter)
	
	def __pipe__(self, inpipe):
		self.inpipes.append(inpipe.outpipe)
	
	def __repr__(self):
		return '<PSorter at %s>' % hex(id(self))


class _PSorter(Stream):
	"""Merge sorted input (smallest to largest) coming from many
	ForkedFeeder's or ProcessPool's.

	All input pipes are polled individually.  When none is ready, the
	collector thread will sleep for a fix duration before polling again.
	"""
	def __init__(self, waittime=0.1):
		self.inpipes = []
		self.waittime = waittime

	def run(self):
		self.inqueues = [Queue.Queue() for _ in xrange(len(self.inpipes))]
		def collect():
			# We make a shallow copy of self.inpipes, as we will be
			# mutating it but still need to refer to the correct queue index
			qindex = copy.copy(self.inpipes).index
			while self.inpipes:
				ready = [p for p in self.inpipes if p.poll()]
				if not ready:
					time.sleep(self.waittime)
				for inpipe in ready:
					item = inpipe.recv()
					self.inqueues[qindex(inpipe)].put(item)
					if item is StopIteration:
						del self.inpipes[self.inpipes.index(inpipe)]
		self.collector_thread = threading.Thread(target=collect)
		self.collector_thread.start()
		
		sorter = ThreadedFeeder(heapq.merge, *__builtin__.map(_iterqueue, self.inqueues))
		self.sorter_thread = sorter.thread
		self.iterator = iter(sorter)
	
	def __pipe__(self, inpipe):
		self.inpipes.append(inpipe.outpipe)
	
	def __repr__(self):
		return '<PSorter at %s>' % hex(id(self))

if sys.platform == "win32":
	PSorter = _PSorter


class QSorter(Stream):
	"""Merge sorted input coming from many ThreadFeeder's or ThreadPool's,
	or anything that has an attribute `outqueue` from which we can get().
	"""
	def __init__(self):
		self.inqueues = []

	def run(self):
		sorter = ThreadedFeeder(heapq.merge, *__builtin__.map(_iterqueue, self.inqueues))
		self.sorter_thread = sorter.thread
		self.iterator = iter(sorter)
	
	def __pipe__(self, inpipe):
		self.inqueue.append(inpipe.outqueue)
	
	def __repr__(self):
		return '<PSorter at %s>' % hex(id(self))


#_____________________________________________________________________
# Useful generator functions


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
	"""Repeatedly call func(*args) and yield the result.  Useful when
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


#_____________________________________________________________________
# Useful curried versions of __builtin__.{max, min, reduce}


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


#_____________________________________________________________________
# main


if __name__ == "__main__":
	import doctest
	if doctest.testmod().failed:
		import sys
		sys.exit(1)
