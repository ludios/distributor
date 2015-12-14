import os
import sys
import json
import click
from collections import defaultdict
from twisted.python import log
from twisted.web.server import Site
from twisted.web.resource import Resource
from twisted.internet import reactor


class NextTaskResource(Resource):
	def __init__(self, taskFile, workerStats):
		Resource.__init__(self)
		self.taskFile = taskFile
		self.workerStats = workerStats

	def render_POST(self, request):
		worker = request.args['worker'][0]
		newStats = self.workerStats.get()
		newStats[worker] += 1
		self.workerStats.set(newStats)
		return json.dumps(self.taskFile.next())


class PersistedValue(object):
	def __init__(self, path, default, transform=lambda v: v):
		"""
		path - file to store value in
		default - default value if no file exists
		transform - (optional) function to transform JSON-decoded value
			to something more useful
		"""
		try:
			self.f = open(path, "rb+")
			self.s = self.f.read()
			# Stripping NULL is safe because we store only JSON
			self.v = transform(json.loads(self.s.rstrip('\x00')))
		except (OSError, IOError):
			self.f = open(path, "wb")
			self.s = ''
			self.set(default)

	def get(self):
		return self.v

	def set(self, v):
		s = json.dumps(v)
		# We might write a value shorter than the previous value, but the system
		# may crash between the write+flush and the truncate call.  To prevent us
		# from seeing a corrupted value after a crash, pad the new value with NULLs
		# to match the length of the previous value.  This should ensure that for
		# values up to 4KB, we don't risk losing a recent value.
		padLength = len(self.s) - len(s)
		if padLength > 0:
			# This will be truncated off immediately after the flush.
			s += ('\x00' * padLength)
		self.f.seek(0)
		self.f.write(s)
		self.f.flush()
		self.f.truncate(len(s))
		self.s = s
		self.v = v


class TaskFile(object):
	def __init__(self, f, dir):
		self.f = f
		self.dir = dir
		self.bytePos = PersistedValue(os.path.join(dir, "byte-pos"), 0)
		self.f.seek(self.bytePos.get())
		log.msg("Reading from {} at position {}".format(self.f, self.bytePos.get()))

	def next(self):
		line = self.f.readline()
		self.bytePos.set(self.f.tell())
		if not line:
			return None
		return line.rstrip('\r\n')


@click.command()
@click.option('--port', default=31000, metavar='PORT',
	help='Listen on PORT (default: 31000).')
@click.option('--interface', default="0.0.0.0", metavar='INTERFACE',
	help='Listen on INTERFACE (default: "0.0.0.0").')
@click.argument('task_file')
@click.argument('dir')
def main(port, interface, task_file, dir):
	log.startLogging(sys.stdout)

	try:
		os.makedirs(dir)
	except OSError:
		pass
	workerStats = PersistedValue(
		os.path.join(dir, "worker-stats"),
		defaultdict(int),
		lambda d: defaultdict(int, d)
	)
	taskFile = TaskFile(open(task_file, 'rb'), dir)

	root = Resource()
	root.putChild("next", NextTaskResource(taskFile, workerStats))
	factory = Site(root)
	reactor.listenTCP(port, factory, interface=interface)
	reactor.run()


if __name__ == '__main__':
	main()
