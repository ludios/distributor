import os
import json
import click
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

class TaskFile(object):
	def __init__(self, f, dir):
		self.f = f
		self.dir = dir
		try:
			self.bytePosFile = open(os.path.join(dir, "byte-pos"), "rb+")
			self.bytePos = int(self.bytePosFile.read().rstrip())
		except (OSError, IOError):
			self.bytePosFile = open(os.path.join(dir, "byte-pos"), "wb")
			self.bytePos = 0
			self.writeBytePos()
		self.f.seek(self.bytePos)

	def writeBytePos(self):
		self.bytePosFile.truncate(0)
		self.bytePosFile.write(str(self.bytePos))
		self.bytePosFile.flush()

	def next(self):
		line = self.f.readline()
		self.bytePos = self.f.tell()
		self.writeBytePos()
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
	import sys
	from twisted.python import log
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
