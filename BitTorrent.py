from pyactor.context import set_context, create_host, sleep, shutdown, interval, later

import random
import copy

#tell = assincronous
#ask = sincronous
#ref = sending reference between machines

#pull = ask for info
#push = nofify with info 

class Peer(object):
	_tell = ["pushGossip","pullGossip","push","init_start","stop_interval","announce"]
	_ask = ["get_name","initArray","get_file","pull"]

	def __init__(self):
		self.torrentFile = []

	def initArray(self,Object):
		self.torrentFile = Object

	def announce(self,tracker,torrent):
		tracker.announce(torrent,self.proxy,10)

	def get_file(self):
		return self.torrentFile

	def get_name(self):
		return self.id

	def pull(self,chunk_id): #will not do it if have the String completed - used in GOSSIP ALGORITHM
		return self.torrentFile[chunk_id]

	def push(self,chunk_id, chunk_data): #nofiy peers with info - used in GOSSIP ALGORITHM
		if self.torrentFile[chunk_id] == None:

			self.torrentFile[chunk_id] = chunk_data
			print self.torrentFile

	def pushGossip(self,torrent):

		torrentPeers = t.getPeers(torrent)

		if self.proxy in torrentPeers:
			randomPeers = random.sample(torrentPeers, 3)

			if self.proxy in randomPeers:

				newPeers = copy.copy(torrentPeers)
				newPeers.pop(self.proxy)
				randomPeers = random.sample(newPeers, 3)

			for peer in randomPeers:
				# PUSH - GOSSIP method:(self send chunk to random peers)
				randomPosition = random.randint(0, 8)
				peer.push(randomPosition, self.torrentFile[randomPosition])


	def pullGossip(self,torrent):

		torrentPeers = t.getPeers(torrent)
		peersNoneChunks = []
		position = 0

		if self.proxy in torrentPeers:
			randomPeer = random.sample(torrentPeers, 1)

			for chunk in self.torrentFile:

				if chunk == None:
					peersNoneChunks.append(position)

				position = position + 1

			if len(peersNoneChunks) > 0:
				randomPosition = random.sample(peersNoneChunks, 1)

				# PULL - GOSSIP method: (self ask for random not None chunk to random peer)
				chunk = randomPeer[0].pull(randomPosition[0],future = True) #use a FUTURE, for not get None chunk
				sleep(0.5)

				if chunk.done():
					try:
						if chunk.result() != None:
							file = self.torrentFile
							file[randomPosition[0]] = chunk.result()
						print "PULL ---------> " + str(chunk.result())
					except Exception, e:
						print e


	#INTERVALS:
	def init_start(self,torrent):
		self.peerAnounce = interval(h, 5, self.proxy, "announce", t,torrent)
		self.peerPush = interval(h, 1, self.proxy, "pushGossip",torrent)

		if self.proxy != ps:
			self.peerPull = interval(h,1, self.proxy, "pullGossip",torrent)

		later(18, self.proxy, "stop_interval")

	def stop_interval(self):
		print "stopping interval"
		self.peerPull.set()
		self.peerPush.set()
		self.peerAnounce.set()

class Tracker(object):
	_tell = ["announce","trackerTimeCheck","init_start","stop_interval"]
	_ask = ["getPeers","getTorrents","getRandomPeers"]

	def __init__(self):
		self.torrents = {}

	def getTorrents(self):
		return self.torrents

	def getRandomPeers(self,torrent_id):
		return random.sample(self.torrents.get(torrent_id),3)

	def getPeers(self,torrent_id):
		return  self.torrents.get(torrent_id)

	def announce(self,torrent_id,peer,time):
		self.torrents.setdefault(torrent_id, {}).update({peer:time})  # {torrent_id: [peer:time , peer2:time]...}

	def trackerTimeCheck(self,torrent):

		deadPeers = {}
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

		later(18, self.proxy, "stop_interval")

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
	p1.init_start("movie")
	p2.init_start("movie")
	p3.init_start("movie")
	p4.init_start("movie")
	p5.init_start("movie")

	t.init_start("movie")

	sleep(20)

	print "-----------"
	print "p1: " + str(p1.get_file())
	print "p2: " + str(p2.get_file())
	print "p3: " + str(p3.get_file())
	print "p4: " + str(p4.get_file())
	print "p5: " + str(p5.get_file())

	print t.getPeers("movie")

shutdown()