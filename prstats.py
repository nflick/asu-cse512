#!/usr/bin/env python

'''Records the run time, and max memory usage of a process. Also records the
number of bytes sent and received on the network during the life of the process.
CSE 512 Course Project
'''

import subprocess
import argparse
import re
import time
import os

class Stats:
	def __init__(self):
		self.start = 0.0
		self.time = 0.0
		self.memory = 0
		self.sent_start = 0
		self.received_start = 0
		self.sent_end = 0
		self.sent_start = 0
		self.sent = 0
		self.received = 0

	def __str__(self):
		return 'Time: {0} s\nMemory: {1} kB\nSent: {2} b\nReceived: {3} b'.format(self.time, self.memory, self.sent, self.received)

def build_parser():
	parser = argparse.ArgumentParser(usage='%(prog)s [-h] [-p PID] [-f FREQUENCY] [COMMANDS...]',
		description='Records the run time and peak memory usage of a process. Also records the number of bytes sent and received on the network during the life of the process.')
	parser.add_argument('-p', '--pid', help='The process ID.')
	parser.add_argument('--frequency', default=20.0, type=float, help='The frequency to check process stats, in milliseconds.')
	return parser

def update_stats(pid, stats):
	with open('/proc/{0}/status'.format(pid), 'r') as f:
		match = re.search(r'VmPeak:\s+(\d+) kB', f.read())
		if match:
			memory = int(match.group(1))
			stats.memory = max(memory, stats.memory)
	with open('/proc/{0}/net/dev'.format(pid), 'r') as f:
		matches = re.findall(r'[\w\d]+:\s+(\d+)\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+(\d+)', f.read())
		received = sum(int(m[0]) for m in matches)
		sent = sum(int(m[1]) for m in matches)
		stats.sent_end = max(sent, stats.sent)
		stats.received_end = max(received, stats.received)

def loop(pid, stats, frequency):
	stats.start = os.stat('/proc/{0}'.format(pid)).st_mtime
	update_stats(pid, stats)
	stats.sent_start = stats.sent_end
	stats.received_start = stats.received_end
	try:
		while (True):
			time.sleep(frequency / 1000)
			update_stats(pid, stats)
	except IOError:
		raise KeyboardInterrupt()

def main():
	parser = build_parser()
	args, cmd = parser.parse_known_args()
	start = time.time()
	stats = Stats()
	sp = None
	print('Type Ctrl-C to finish.')
	try:
		if args.pid is None:
			sp = subprocess.Popen(cmd)
			pid = str(sp.pid)
		else:
			pid = args.pid
		loop(pid, stats, args.frequency)
	except KeyboardInterrupt:
		if sp is not None:
			sp.terminate()
		stats.time = time.time() - stats.start
		stats.sent = stats.sent_end - stats.sent_start
		stats.received = stats.received_end - stats.received_start
		print()
		print(stats)
	except Exception as ex:
		parser.error(str(ex))

if __name__ == '__main__':
	main()
