#!/usr/bin/env python

import urllib2
from stream import AsyncThreadPool

URLs = ['http://www.cnn.com/',
	'http://www.bbc.co.uk/',
	'http://www.economist.com/',
	'http://slashdot.org/',
	'http://reddit.com/',
	'http://news.ycombinator.com/',
]

def urlread(urls, timeout=10):
	for url in urls:
		yield url, urllib2.urlopen(url, timeout=timeout).read()

if __name__ == '__main__':
	## Use 4 threads to retrieve URLs concurrently
	retrieved = URLs >> AsyncThreadPool(urlread, poolsize=4)
	for url, content in retrieved:
		print '%r is %d bytes' % (url, len(content))
