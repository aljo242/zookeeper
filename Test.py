from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging

def myListener(state):
	if state == KazooState.LOST:
		# register connection lost
		logging.critical("connection is LOST")
	elif state == KazooState.SUSPENDED:
		# Handle being disconnected from zookeeper
		logging.critical("connection is SUSPENDED")
	else:
		# handle being connected/reconnected to zookeeper
		logging.critical("connection is ...")


class Server():
	def __init__(self):
		self.electionPrefix = "/election/"



electionPrefix = "/election/"
numNodes = 3
electionNodeList = []
leaderNode = -1

# set to debug for now
logging.basicConfig(level = logging.DEBUG)

zk = KazooClient(hosts = '127.0.0.1:2181')
zk.start()
zk.add_listener(myListener)

zk.ensure_path(electionPrefix)
for _ in range(numNodes):
	path = electionPrefix
	logging.debug(f"Creating node: {path}")
	zk.create(path, value = b"val", acl= None, ephemeral=True, sequence=True, makepath=False)
print("YOOOO")

children = zk.get_children(electionPrefix)
for child in children:
	logging.debug(child)
	electionNodeList.append(child)

leaderNode = min(electionNodeList)
logging.critical(f"LEADER NODE IS: {leaderNode}")


for node in electionNodeList:
	nodePath = electionPrefix + str(node)
	logging.debug(f"Deleting: {nodePath}...")
	zk.delete(nodePath)


zk.stop()