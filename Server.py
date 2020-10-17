from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging

# set to debug for now
logging.basicConfig(level = logging.DEBUG)

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
    def __init__(self, zk_connection):
        self.electionPrefix = "/election/"
        self.clientPrefix = "/client/"
        self.zk = zk_connection
        self.electionNodeList = []
        self.leaderNode = -1
        self.ID = -1

        self.dictionary = {}
        
        # ensure election path exists
        self.zk.ensure_path(self.electionPrefix)
        
        # check children at node
        self.electionNodeList = self.zk.get_children(self.electionPrefix)
        for node in self.electionNodeList:
            logging.critical(node)

        self.ID = self.zk.create(self.electionPrefix, value = b"val", acl= None, ephemeral=False, sequence=True, makepath=False)
        #logging.critical(f"SERVER ID IS: {self.ID}")

        # check children at node
        self.electionNodeList = self.zk.get_children(self.electionPrefix)


        self.leaderNode = self.electionPrefix + min(self.electionNodeList)
        logging.critical(f"CURRENT LEADER NODE: {self.leaderNode}")

        if self.leaderNode == self.ID: # if i am the leader
            self.zk.set(self.ID, b"leader")

        
    def __del__(self):
        #logging.critical(f"Deleting server: {self.ID}...")
        self.zk.delete(self.ID, recursive=True)

    def Read(self, key):
        return self.dictionary[key]

    def Add_Update(self, key, value):
        self.dictionary[key] = value

if __name__ == "__main__":
    zk = KazooClient(hosts = '127.0.0.1:2181')
    zk.start()
    zk.add_listener(myListener)

    server1 = Server(zk)
    server2 = Server(zk)
    server3 = Server(zk)

    del server1
    del server2
    del server3

    zk.stop()