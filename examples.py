from stream import *
import re

# Server log processing

result = []
pattern = re.compile('[Pp]attern')
for line in open('log'):
	if pattern.search(line):
		result += line.strip()

vs.

result = open('log').xreadlines() >> filter(re.compile('[Pp]attern').search) >> mapmethod('strip') >> list
