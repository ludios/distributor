import os
import sys
import json
import click
from twisted.python import log
from twisted.web.server import Site
from twisted.web.resource import Resource
from twisted.internet import reactor


class Next(Resource):
	def __init__(self, taskFile):
		Resource.__init__(self)
		self.taskFile = taskFile

	def render_POST(self, request):
		worker = request.args['worker'][0]
		return json.dumps(self.taskFile.next())


class PersistedValue(object):
	def __init__(self, p, default):
		try:
			self.f = open(p, "rb+")
			self.s = self.f.read()
			# Stripping NULL is safe because we store only JSON
			self.v = json.loads(self.s.rstrip('\x00'))
		except (OSError, IOError):
			self.f = open(p, "wb")
			self.set(default)

	def get(self):
		return self.v

	def set(self, v):
		if v == self.v:
			return
		s = json.dumps(v)
		padLength = len(self.s) - len(s)
		if padLength > 0:
			# This will be truncated off immediately after the flush
			s += ('\x00' * padLength)
		self.f.seek(0)
		# We might write a value shorter than the last value, but the filesystem/system
		# may crash between the write+flush and the truncate call.  To prevent us from
		# seeing a corrupted value after crashes, pad the new value with NULLs if necessary.
		# This should ensure that values up to 4KB don't risk of losing a recent value.
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
	taskFile = TaskFile(open(task_file, 'rb'), dir)

	root = Resource()
	root.putChild("next", Next(taskFile))
	factory = Site(root)
	reactor.listenTCP(port, factory, interface=interface)
	reactor.run()


if __name__ == '__main__':
	main()
