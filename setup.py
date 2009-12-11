#!/usr/bin/env python

import os, sys
from distutils.core import setup

sys.path.insert(0, os.path.dirname(__file__))
try:
    import stream
finally:
    del sys.path[0]

classifiers = """
Development Status :: 3 - Alpha
Intended Audience :: Developers
License :: OSI Approved :: MIT License
Operating System :: OS Independent
Programming Language :: Python
Topic :: Software Development :: Libraries :: Python Modules
Topic :: Utilities
"""

setup(
	name = 'stream',
	version = stream.__version__,
	description = stream.__doc__.split('\n')[0],
  long_description = ''.join(open('stream.py').readlines()[2:110]),
	author = 'Anh Hai Trinh',
	author_email = 'moc.liamg@hnirt.iah.hna:otliam'[::-1],
  keywords='lazy iterable iterator generator stream data flow functional processing',
	url = 'http://github.com/aht/stream.py',
	platforms=['any'],
	classifiers=filter(None, classifiers.split("\n")),
	py_modules = ['stream']
)
