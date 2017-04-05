from pyactor.context import set_context, create_host, sleep, shutdown, interval, later

import plotly.plotly as py
import plotly.graph_objs as go

import plotly
import random
import copy


#tell = assincronous
#ask = sincronous
#ref = sending reference between machines

#pull = ask for info
#push = nofify with info 

class Peer(object):
	_tell = ["pushGossip","pullGossip","push","init_start","stop_interval","announce"]
	_ask = ["initArray","get_file","pull","chunk_rounds"]

	def __init__(self):
		self.torrentFile = []
		self.torrentPeers = {}
		self.chunks = []

	def initArray(self,Object):
		self.torrentFile = Object

	def announce(self,tracker,torrent):
		tracker.announce(torrent,self.proxy,10) #ref

	def get_file(self):
		return self.torrentFile

	def chunk_rounds(self):
		return self.chunks

	def pull(self,chunk_id):
		return self.torrentFile[chunk_id]

	def push(self,chunk_id, chunk_data):
		if self.torrentFile[chunk_id] == None:
			self.torrentFile[chunk_id] = chunk_data
			print self.id +  str(self.torrentFile)

	def pushGossip(self,torrent): #deliver
		self.torrentPeers = t.getPeers(torrent)
		self.chunks.append(sum(x is not None for x in self.torrentFile))

		if self.torrentPeers != None:

			if self.proxy in self.torrentPeers:
				randomPeers = random.sample(self.torrentPeers, 2)

				for peer in randomPeers:
					if self.proxy in randomPeers:

						newPeers = copy.copy(self.torrentPeers)
						newPeers.pop(self.proxy)
						randomPeers = random.sample(newPeers, 2)

					sleep(0.2)
					# PUSH - GOSSIP method:(self send chunk to random peer of randoms)
					randomPosition = random.randint(0, 8)
					peer.push(randomPosition, self.torrentFile[randomPosition])


	def pullGossip(self,torrent): #ask
		self.torrentPeers = t.getPeers(torrent)
		if self.torrentPeers != None:

			self.peersNoneChunks = []
			self.position = 0

			if self.proxy in self.torrentPeers:
				randomPeers = random.sample(self.torrentPeers, 2)

				for randomPeer in randomPeers:
					self.position = 0
					for chunk in self.torrentFile:

						if chunk == None:
							self.peersNoneChunks.append(self.position)

						self.position = self.position + 1

					if len(self.peersNoneChunks) > 0:
						randomPosition = random.sample(self.peersNoneChunks, 1)

						# PULL - GOSSIP method: (self ask for random not None chunk to random peer)
						chunk = randomPeer.pull(randomPosition[0],future = True) #use a FUTURE, for not get None chunk

						sleep(0.3)
						if chunk.done():
							try:
								if chunk.result() != None:
									file = self.torrentFile
									file[randomPosition[0]] = chunk.result()
								print "PULL " + self.id + " ---------> " + str(chunk.result())
							except Exception, e:
								print e

	#INTERVALS:
	def init_start(self,torrent):
		self.peerAnounce = interval(h, 6, self.proxy, "announce", t,torrent)
		self.peerPush = interval(h, 1, self.proxy, "pushGossip",torrent)


		if self.proxy != ps:
			self.peerPull = interval(h, 1, self.proxy, "pullGossip",torrent)

		later(60, self.proxy, "stop_interval")

	def stop_interval(self):
		print "stopping interval"
		self.peerPull.set()
		self.peerPush.set()
		self.peerAnounce.set()

class Tracker(object):
	_tell = ["announce","trackerTimeCheck","init_start","stop_interval"]
	_ask = ["getPeers"]

	def __init__(self):
		self.torrents = {}

	def getPeers(self,torrent_id):
		return  self.torrents.get(torrent_id)

	def announce(self,torrent_id,peer,time):
		self.torrents.setdefault(torrent_id, {}).update({peer:time})  # {torrent_id: [peer:time , peer2:time]...}

	def trackerTimeCheck(self,torrent):

		deadPeers = {}
		if self.torrents.get(torrent) != None:
			for peer in  self.torrents.get(torrent):

				time = self.torrents.get(torrent)[peer]
				self.announce(torrent, peer, time - 1)

				if time < 1:
					deadPeers.setdefault(torrent, []).append(peer)

			for torrent in deadPeers.keys():
				for peer in deadPeers.get(torrent):
					self.torrents.get(torrent).pop(peer,None)



	#INTERVALS:
	def init_start(self,torrent):
		self.timeCheck = interval(h,1, self.proxy, "trackerTimeCheck",torrent)

		later(60, self.proxy, "stop_interval")

	def stop_interval(self):
		print "stopping interval"
		self.timeCheck.set()


if __name__ == '__main__':

	set_context()
	h = create_host()

	t = h.spawn("tracker1",Tracker)

	ps = h.spawn("peerSeed",Peer)
	p1 = h.spawn("peer1",Peer)
	p2 = h.spawn("peer2", Peer)
	p3 = h.spawn("peer3",Peer)
	p4 = h.spawn("peer4", Peer)
	p5 = h.spawn("peer5", Peer)

	ps.initArray(["G", "O", "T", "O", "R", "R", "E", "N", "T"])
	p1.initArray([None, None, None, None, None, None, None, None, None])
	p2.initArray([None, None, None, None, None, None, None, None, None])
	p3.initArray([None, None, None, None, None, None, None, None, None])
	p4.initArray([None, None, None, None, None, None, None, None, None])
	p5.initArray([None, None, None, None, None, None, None, None, None])

	ps.init_start("movie")
	sleep(0.1)
	p1.init_start("movie")
	sleep(0.1)
	p2.init_start("movie")
	sleep(0.1)
	p3.init_start("movie")
	sleep(0.1)
	p4.init_start("movie")
	sleep(0.1)
	p5.init_start("movie")
	sleep(0.1)
	t.init_start("movie")

	sleep(65)

	print "-----------"
	print "p1: " + str(p1.get_file())
	print "p1: " + str(p1.chunk_rounds())

	print "p2: " + str(p2.get_file())
	print "p2: " + str(p2.chunk_rounds())

	print "p3: " + str(p3.get_file())
	print "p3: " + str(p3.chunk_rounds())

	print "p4: " + str(p4.get_file())
	print "p4: " + str(p4.chunk_rounds())

	print "p5: " + str(p5.get_file())
	print "p5: " + str(p5.chunk_rounds())

	plotly.tools.set_credentials_file(username='ArnauMarti', api_key='UZWivMWZggFo2kO5WezX')

	x1 =	list(range(len(p1.chunk_rounds())))
	y1 = p1.chunk_rounds()

	x2 =	list(range(len(p2.chunk_rounds())))
	y2 = p2.chunk_rounds()

	x3 =	list(range(len(p3.chunk_rounds())))
	y3 = p3.chunk_rounds()

	x4 =	list(range(len(p4.chunk_rounds())))
	y4 = p4.chunk_rounds()

	x5 =	list(range(len(p5.chunk_rounds())))
	y5 = p5.chunk_rounds()

	# Create a trace
	trace0 = go.Scatter(
		x=x1,
		y=y1,
		name = "peer1"
	)
	trace1 = go.Scatter(
		x=x2,
		y=y2,
		name = "peer2"
	)
	trace2 = go.Scatter(
		x=x3,
		y=y3,
		name = "peer3"
	)
	trace3 = go.Scatter(
		x=x4,
		y=y4,
		name = "peer4"
	)
	trace4 = go.Scatter(
		x=x5,
		y=y5,
		name = "peer5"
	)

	data = [trace0, trace1, trace2,trace3,trace4]
	# Edit the layout
	layout = dict(title='pull/push-2',
				  xaxis=dict(title='Time'),
				  yaxis=dict(title='Chunk'),
				  )

	fig = dict(data=data, layout=layout)
	py.iplot(fig, filename='PullPush2')


shutdown()