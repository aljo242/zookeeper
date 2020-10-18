from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging

# set to debug for now
logging.basicConfig(level = logging.ERROR)

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
        self.updateElectionNodeList()


        self.ID = self.zk.create(self.electionPrefix, value = b"val", acl= None, ephemeral=False, sequence=True, makepath=False)
        print(f"New Server Connection: {self.ID}")

        # check children at node
        self.updateElectionNodeList()
        self.selectLeader()
        print("Current Connected Election Servers:")
        for node in self.electionNodeList:
            print(f"\t{node}")

    def updateElectionNodeList(self):
        self.electionNodeList = self.zk.get_children(self.electionPrefix)
        index = 0
        for node in self.electionNodeList:
            newNode = self.electionPrefix + node
            self.electionNodeList[index] = newNode
            index += 1

    def selectLeader(self):
        self.leaderNode = min(self.electionNodeList)
        print(f"ELECTED LEADER NODE: {self.leaderNode}")
        if self.leaderNode == self.ID: # if i am the leader
            self.zk.set(self.ID, b"leader")

    def __del__(self):
        print(f"Deleting server: {self.ID}...")
        self.updateElectionNodeList()
        self.selectLeader()
        # elect the new node if leader is dying
        if self.ID == self.leaderNode:
            print("Leader Node is being shut down...")
            self.electionNodeList.remove(self.ID)
            if len(self.electionNodeList) > 0:
                self.selectLeader()
                self.zk.set(self.leaderNode, b'leader')
                self.zk.set(self.ID, b'invalid')
                print("New list of election connections:")
                for node in self.electionNodeList:
                    print(f"\t{node}")
        # before deleting node, send all info to the new leader
                keys = self.zk.get_children(self.ID)
                if keys != None:
                    for key in keys:
                        path = self.ID + '/' + key
                        value = self.zk.get(path)[0]
                        newPath = self.leaderNode + '/' + key
                        self.zk.ensure_path(newPath)
                        self.zk.set(newPath, value)

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