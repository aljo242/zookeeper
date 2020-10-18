from kazoo.client import KazooClient
from kazoo.client import KazooState
import socket
import logging

# set to debug for now
logging.basicConfig(level = logging.ERROR)

"""
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
"""

def UpdateWatcher(event):
    print("UPDATE WATCH!!!!!!!!")
    print(event)

def ReadWatcher(event):
    print("READ WATCH!!!!!!!!")    
    print(event)




class Client:
    def __init__(self, zk_connection):
        self.electionPrefix = "/election/"
        self.clientPrefix = "/client/"
        self.zk = zk_connection
        self.electionNodeList = []
        self.leaderNode = -1
        self.ID = -1

        self.dictionary = {}
        
        # ensure election path exists
        self.zk.ensure_path(self.clientPrefix)
        
        self.ID = self.zk.create(self.clientPrefix, value = b"val", acl= None, ephemeral=True, sequence=True, makepath=True)
        print(self.ID)

        # check children at node
        self.getElectionServers()
        self.leaderNode = self.findLeader()
        logging.critical(f"CLIENT {self.ID}, LEADER NODE: {self.leaderNode}")
 

    def Read(self, key):
        path = self.leaderNode + '/' + key
        if self.zk.exists(path):
            val = self.zk.get(path, watch=ReadWatcher)[0]
            print(val)
            self.dictionary[key] = val
            return val

        return None

    # check if znode exists under current leader node
    # if exists, set at that node
    # if not, create a znode
    def Add_Update(self, key, value):
        self.dictionary[key] = value
        path = self.leaderNode + '/' + key
        self.zk.ensure_path(path)
        keys = self.zk.get_children(self.leaderNode)
        print(f"SET PATH IS: {path}")
        print("Printing Keys...")
        for key in keys: 
            print(key)
        self.zk.set(path, value)

        keys = self.zk.get_children(self.leaderNode)
        print(f"SET PATH IS: {path}")
        print("Printing NEW Keys...")
        for key in keys: 
            print(key)


    def findLeader(self):
        for node in self.electionNodeList:
            val = self.zk.get(node)[0]
            if val == b'leader':
                return node

    def getElectionServers(self):
        self.electionNodeList = self.zk.get_children(self.electionPrefix)
        index = 0
        for node in self.electionNodeList:
            newNode = self.electionPrefix + node
            self.electionNodeList[index] = newNode
            index += 1
            

