:mod:`stream` -- Lazily-evaluated, parallelizable pipeline
============================================================================

.. module:: stream
   :synopsis: Lazily-evaluated, parallelizable pipeline
.. moduleauthor:: Anh Hai Trinh <anh.hai.trinh@gmail.com>


Streams are iterables with a pipelining mechanism to enable data-flow
programming and easy parallelization.

The idea is to take the output of a function that turns an iterable into
another iterable and plug that as the input of another such function.
While you can already do this using function composition, this package
provides an elegant notation for it by overloading the ``>>`` operator.

This approach focuses the programming on processing streams of data, step
by step.  A pipeline usually starts with a producer, then passes through
a number of processors.  Multiple streams can be branched and combined.
Finally, the output is fed to an accumulator, which can be any function
of one iterable argument.  The paradigm is very much inspired by this chapter
from the wizard book: <http://mitpress.mit.edu/sicp/full-text/sicp/book/node69.html>.

**Producers**:  anything iterable
	+ from this module: :func:`seq`, :func:`gseq`, :func:`repeatcall`, :func:`chaincall`

**Processors**:
	+ by index: :func:`take`, :func:`drop`, :func:`takei`, :func:`dropi`
	+ by condition: :func:`filter`, :func:`takewhile`, :func:`dropwhile`
	+ by transformation: :func:`apply`, :func:`map`, :func:`fold`
	+ by combining streams: :func:`prepend`, :func:`tee`
	+ for special purpose: :func:`chop`, :data:`cut`, :data:`flatten`

**Accumulators**:  any function callable on an iterable
   + from this module: :data:`item`, :func:`maximum`, :func:`minimum`, :func:`~stream.reduce`
   + from Python: :func:`list`, :func:`sum`, :func:`dict`, :func:`max`, :func:`min` ...

Values are computed only when an accumulator forces some or all evaluation
(not when the stream are set up).

All parts of a pipeline can be **parallelized** using threads or processes.  A
blocking producer can be fed from another thread or process by a
:class:`ThreadedFeeder` or :class:`ForkedFeeder`.  An input stream can be
distributed to a :class:`ThreadPool` or :class:`ProcessPool` -- both use
multiple workers to process the input simultaneously.  An :class:`Executor`
provides fine-grained job control over such worker pool.  Concurrent streams
can be accumulated into a single output using a :class:`PCollector` or
:class:`QCollector` -- or if they are already sorted and needs merging, using a
:class:`PSorter` or :class:`QSorter`.  


Generators
----------

.. function:: seq([start=0, step=1])

    An arithmetic sequence generator.

    Works with any type with ``+`` defined.

    >>> seq(1, 0.25) >> item[:10]
    [1, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5, 2.75, 3.0, 3.25]

.. function:: gseq(ratio[, initval=1])

   A geometric sequence generator.

   Works with any type with ``*`` defined.

   >>> from decimal import Decimal
   >>> gseq(Decimal('.2')) >> item[:4]
   [1, Decimal('0.2'), Decimal('0.04'), Decimal('0.008')]

.. function:: repeatcall(func[, \*args, \*\*kwargs])

   Repeatedly call `func(\*args, \*\*kwargs)` and yield the result.

   Useful when `func(\*args, \*\*kwargs)` returns different results, esp.
   randomly.

.. function:: chaincall(func, initval)

   Yield `func(initval)`, `func(func(initval))`, etc.


Processors
----------

All processors take an iterable or a :class:`Stream` instance and return a
:class:`Stream` instance.

.. function:: take(n)

   Take the first `n` items of the input stream.

.. function:: drop(n)

   Drop the first `n` elements of the input stream.

.. function:: takei(indices)

   Take elements of the input stream by index.

   `indices` should be an iterable over the list of indices to be taken.

.. function:: dropi(indices)

   Drop elements of the input stream by index.

   `indices` should be an iterable over the list of indices to be dropped.

.. function:: chop(n)

   Chop the input stream into segments of length `n`.
    
   >>> range(10) >> chop(3) >> list
   [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]

.. data:: cut

   Slice each element of the input stream.
    
   >>> [range(10), range(10, 20)] >> cut[::2] >> list
   [[0, 2, 4, 6, 8], [10, 12, 14, 16, 18]]

   See also: :data:`item`, which slices the input stream as a whole.

.. data:: flatten

   Flatten a nested stream of arbitrary depth.

   >>> (xrange(i) for i in seq(step=3)) >> flatten >> item[:18]
   [0, 1, 2, 0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5, 6, 7, 8]

.. function:: filter(function)

   Filter the input stream, selecting only values which evaluates to True
   by the given `function`, à la :func:`itertools.ifilter`.

   >>> even = lambda x: x%2 == 0
   >>> range(10) >> filter(even) >> list
   [0, 2, 4, 6, 8]

.. function:: takewhile(function)

   Take items from the input stream that come before the first item to
   evaluate to False by the given `function`, à la :func:`itertools.takewhile`.

.. function:: dropwhile(function)

   Drop items from the input stream that come before the first item to evaluate
   to False by the given `function`, à la :func:`itertools.dropwhile`.

.. function:: apply(function)

   Invoke `function` using each element of the input stream unpacked as
   its argument list and yield each result, à la :func:`itertools.starmap`.
    
   >>> vectoradd = lambda u,v: zip(u, v) >> apply(lambda x,y: x+y) >> list
   >>> vectoradd([1, 2, 3], [4, 5, 6])
   [5, 7, 9]


.. function:: map(function)
 
   Invoke `function` using each element of the input stream as its only
   argument and yield each result, a la :func:`itertools.imap`.
 
   >>> square = lambda x: x*x
   >>> range(10) >> map(square) >> list
   [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

.. function:: fold(function[, initval])

   Combines the elements of the input stream by applying a function of two
   arguments to a value and each element.  At each step, the value is set
   to the result of the function application and it is also yielded.  The effect
   of fold is an accumulation.
   
   :param function: a function of two arguments.
   :param initval: used as the starting value if supplied.

   This example calculate a few partial sums of the series 1 + 1/2 + 1/4 +...

   >>> gseq(0.5) >> fold(lambda x, y: x + y) >> item[:5]
   [1, 1.5, 1.75, 1.875, 1.9375]

.. function:: prepend(iterable)

   Inject values of `iterable` at the beginning of a (possibly infinite) input stream.

.. function:: tee(named_stream)

   Make a T-split of the input stream.

   :param named_stream: a :class:`Stream` object toward which the split branch will be piped.
 
   >>> foo = filter(lambda x: x%3==0)
   >>> bar = seq(0, 2) >> tee(foo)
   >>> bar >> item[:5]
   [0, 2, 4, 6, 8]
   >>> foo >> item[:5]
   [0, 6, 12, 18, 24]


Accumulators
------------

.. data:: item

   Slice the input stream, return a list.

   >>> i = itertools.count()
   >>> i >> item[:10:2]
   [0, 2, 4, 6, 8]
   >>> i >> item[:5]
   [10, 11, 12, 13, 14]

   Negative values are also possible (all evaluation will be forced).

   >>> xrange(20) >> item[::-2]
   [19, 17, 15, 13, 11, 9, 7, 5, 3, 1]
   
   See also: :data:`cut`, which slices each stream element individually.

.. function:: maximum(key=function)

   Curried version of the built-in :func:`max`.
    
   >>> Stream([3, 5, 28, 42, 7]) >> maximum(lambda x: x%28) 
   42

.. function:: minimum(key=function)

   Curried version of the built-in :func:`min`.

   >>> Stream([[13, 52], [28, 35], [42, 6]]) >> minimum(lambda v: v[0] + v[1])
   [42, 6]

.. function:: stream.reduce(function, initval=None)

   Curried version of the built-in :func:`reduce`.
    
   >>> reduce(lambda x,y: x+y)( [1, 2, 3, 4, 5] )
   15


Parallelization
---------------

Not only is it possible to parallelize all parts of linear pipelines, the
primitives provided here should make it easy to implement many parallel
processing patterns: multiple producers --- single consumer,
single producer --- multiple consumers, many--to--many map/reduce, etc.


Feeders
^^^^^^^

When a producer is doing blocking I/O, it is possible to use a
:class:`ThreadedFeeder` or :class:`ForkedFeeder` to improve performance.  The
feeder will start a thread or a process to run the producer and feed generated
items back to the pipeline, thus minimizing the time that the whole pipeline has
to wait when the producer is blocking in system calls.

In both case, a feeder object is an iterable that is safe to use by many threads.


.. class:: ForkedFeeder(generator[, \*args, \*\*kwargs])
   
   Create a feeder that run the given generator with `\*args` and `\*\*kwargs`
   in a child process.  The feeder will act as an eagerly evaluating proxy of
   the generator.


.. class:: ThreadedFeeder(generator[, \*args, \*\*kwargs])
   
   Create a feeder that run the given generator with `\*args` and `\*\*kwargs`
   in a separate thread.  The feeder will act as an eagerly evaluating proxy of
   the generator.


Pools of workers
^^^^^^^^^^^^^^^^

If the order of output does not matter given an input stream, a
:class:`ThreadPool` or :class:`ProcessPool` can be used to speed up the task.
They both utilize a number of workers in other threads or processes to work on
items pulled from the input stream asynchronously.

An instantiated pool object is an iterable derived from :class:`Stream` and
represents the output values. The returned iterator behaves as follow: their
:func:`next` calls return as soon as a next output value is available, or raise
:exc:`StopIteration` if there is no more output.  A pool object can also be
futher piped.
 
If an input `value` causes an :exc:`Exception` to be raised in the worker
thread/process, the tuple `(value, exception)` is put into the pool's
`failqueue`.  The attribute `failure` is a thead-safe iterator over the
`failqueue`.

A pool with one worker outputs values synchronously in the order of input.


.. class:: ProcessPool(function[, poolsize, args=[], kwargs={}])

   Distribute a stream processing `function` to a pool of worker threads.
   
   :param function: an iterator-processing function, one that takes an iterator and returns an iterator.
   :param poolsize: the number of worker processes, default to the number of CPUs.
   
   >>> range(10) >> ProcessPool(map(lambda x: x*x)) >> sum
   285


.. class:: ThreadPool(function[, poolsize, args=[], kwargs={}])

   Distribute a stream processing `function` to a pool of worker threads.

   :param function: an iterator-processing function, one that takes an iterator and returns an iterator.
   :param poolsize: the number of worker threads, default to the number of CPUs.
   
   >>> range(10) >> ThreadPool(map(lambda x: x*x)) >> sum
   285


Executor
^^^^^^^^

An :class:`Executor` provide an API to perform fine-grained, concurrent
job control over a thread/process pool.  

.. class:: Executor(poolclass, function[, poolsize, args=[], kwargs={}])

   Distribute a stream processing `function` to a pool of workers, providing an
   API for job submission and cancellation.

   :param poolclass: either :class:`ThreadPool` or :class:`ProcessPool`.
   :param function: an iterator-processing function, one that takes an iterator and returns an iterator.
   :param poolsize: the number of workers, default to the number of CPUs.

   :attribute result: an iterator over the result
   :attribute failure: an iterator of `(badvalue, exception)` raised

   An instantiated Executor is safe to use by many threads.

   The `result` and `failure` attributes are :class:`Stream` instances and thus
   iterable.  The returned iterators behave as follow: their :func:`next`
   calls will return as soon as a next output is available, or raise
   :exc:`StopIteration` if there is no more output.

   .. method:: submit(\*items)

      Submit jobs items to be processed.
      
      Return job ids assigned to the submitted items.

   .. method:: cancel(\*ids)

      Try to cancel jobs with associated ids.
       
      Return the actual number of jobs cancelled.

   .. method:: status(\*ids)

      Return the statuses of jobs with associated ids at the
      time of call.  
      
      Valid statuses are: ``'SUBMITED'``, ``'CANCELLED'``, ``'RUNNING'``, 
      ``'COMPLETED'`` or ``'FAILED'``.

   .. method:: close()
   
      Signal that the executor will no longer accept job submission.
    
      Worker threads/processes will be allowed to terminate after all jobs have
      been completed.  Without a call to :func:`close`, they will stay around
      forever waiting for more jobs to come.

   .. method:: shutdown()

      Shut down the Executor.  Suspend all waiting jobs.
    
      Running workers will terminate after finishing their current job items.
      The call will block until all workers die.


Mergers
^^^^^^^

Multiple concurrent streams can be piped to a single :class:`PCollector` or
:class:`QCollector`, which will gather generated items whenever they are
available.  PCollectors can collect from :class:`ForkedFeeder`'s or
:class:`ProcessPool`'s (via system pipes) and QCollector's can collect from
:class:`ThreadedFeeder`'s and :class:`ThreadPool`'s (via queues).

:class:`PSorter` and :class:`QSorter` are also collectors, but given multiples
sorted input streams (low to high), a Sorter will output items in sorted order.

All merger objects are iterables derived from :class:`Stream` and
represent the output values.  They can also be further piped.


.. class:: PCollector([waittime=0.1])

   Collect items from many :class:`ForkedFeeder`'s or :class:`ProcessPool`'s.

   .. note:: On POSIX systems, PCollector uses the :manpage:`select(2)` system
      call and does not understand the `waittime` parameter.  On Windows,
      PCollector has to poll each input pipe individually and if none is ready,
      it goes to sleep for a fix duration given by `waittime` (default to 0.1s).


.. class:: QCollector([waittime=0.1])
   
	Collect items from many :class:`ThreadedFeeder`'s or :class:`ThreadPool`'s.

	All input queues are polled individually.  When none is ready, the
	collector goes to sleep for a fix duration given by the parameter `waittime`.


.. class:: PSorter()

   Merge sorted input (smallest to largest) coming from many
   :class:`ForkedFeeder`'s or :class:`ProcessPool`'s.

   Piping to a PSorter registers the input stream as a source to be sorted.


.. class:: QSorter()

   Merge sorted input (smallest to largest) coming from many
   :class:`ThreadedFeeder`'s or :class:`ThreadPool`'s.

   Piping to a QSorter registers the input stream as a source to be sorted.


How it works
------------

:class:`Stream` is the base class of most others in the module.  A Stream object
is both a lazy list of items and an iterator-processing function.  A Stream
processor when instantiated usually represents an empty iterator which will
be replaced when an input stream is piped into it.


.. class:: Stream(iterable)
   
   Make a Stream object from an iterable.

   The outgoing stream is represented by the attribute `iterator`.

   The iterator-processing function is represented by the method
   :meth:`__call__`, which should return a new iterator representing
   the output of the Stream.

   A Stream subclass will usually implement :meth:`__call__`, unless it is an
   accumulator and will not return a Stream, in which case it will need to
   implement :meth:`__pipe__`.

   The ``>>`` operator works as follow: the expression ``a >> b`` means
   ``b.__pipe__(a) if hasattr(b, '__pipe__') else b(a)``.

   .. method:: __call__(self, iterator)

      An iterator-processing function, one that takes an iterator
      and returns an iterator.

      The default behavior is to chain `iterator` with `self.iterator`,
      in effect append `self` to the input stream in.

      >>> [1, 2, 3] >> Stream([4, 5, 6]) >> list
      [1, 2, 3, 4, 5, 6]

   .. method:: __pipe__(self, inpipe)

      Defines the connection mechanism between `self` and `inpipe`.

      By default, it replaces `self.iterator` with the one returned by
      ``self.__call__(iter(inpipe))``.

The following are constructors of :class:`Stream`-derived classes: :func:`take`,
:func:`drop`, :func:`takei`, :func:`dropi`, :func:`chop`, :func:`filter`,
:func:`takewhile`, :func:`dropwhile`, :func:`apply`, :func:`map`, :func:`fold`,
:func:`prepend`, :func:`tee`, :class:`ProcessPool`, :class:`ThreadPool`,
:class:`PCollector`, :class:`QCollector`, :class:`PSorter`, :class:`QSorter`.

The following are singleton objects of :class:`Stream`-derived classes:
:data:`item`, :data:`cut`, :data:`flatten`.


Examples
--------

String processing
^^^^^^^^^^^^^^^^^
Grep some lines matching a regex from a file, cut out the 4th field
separated by " ", ":" or ".", then save as a list::

   import re
   from stream import filter, map, cut

   result = open('file') \
      >> filter(re.compile(regex).search) \
      >> map(re.compile(' |:|\.').split) \
      >> cut[3] \
      >> list


Feeding a blocking producer
^^^^^^^^^^^^^^^^^^^^^^^^^^^
Demonstrate the use of a :class:`ThreadedFeeder` to minimize time spent by the
whole pipeline waiting for a blocking producer.
::

   import time
   import operator
   from stream import ThreadedFeeder, map

   def blocking_producer():
      for n in range(100):
         time.sleep(0.05)
         yield 42

   if __name__ == '__main__':
      f = lambda x: x**x**3
      print ThreadedFeeder(blocking_producer) >> map(f) >> sum


Retrieving web pages concurrently
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Demonstrate the use of a :class:`ThreadPool` to simultaneously retrieve web
pages:
::

   import urllib2
   from stream import ThreadPool

   URLs = [
      'http://www.cnn.com/',
      'http://www.bbc.co.uk/',
      'http://www.economist.com/',
      'http://nonexistant.website.at.baddomain/',
      'http://slashdot.org/',
      'http://reddit.com/',
      'http://news.ycombinator.com/',
   ]

   def retrieve(urls, timeout=10):
      for url in urls:
         yield url, urllib2.urlopen(url, timeout=timeout).read()

   if __name__ == '__main__':
      retrieved = URLs >> ThreadPool(retrieve, poolsize=4)
      for url, content in retrieved:
         print '%r is %d bytes' % (url, len(content))
      for url, exception in retrieved.failure:
         print '%r failed: %s' % (url, exception)

Alternatively, you could use a :class:`ProcessPool`.

Resources
---------

The code repository is located at <http://github.com/aht/stream.py>.

Articles written by the author can be retrieved from
<http://blog.onideas.ws/tag/project:stream.py>.
