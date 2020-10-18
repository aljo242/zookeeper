from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging

# set to debug for now
logging.basicConfig(level = logging.ERROR)





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
        print(f"New client connection: {self.ID}")

        # check children at node
        self.getElectionServers()
        self.leaderNode = self.findLeader()
 

    def Read(self, key):
        path = self.leaderNode + '/' + key
        if self.zk.exists(path):
            val = self.zk.get(path)[0]
            #print(val)
            self.dictionary[key] = val
            return val

        return None

    # check if znode exists under current leader node
    # if exists, set at that node
    # if not, create a znode
    def Add_Update(self, key, value):
        self.dictionary[key] = value
        path = self.leaderNode + '/' + key
        #print(f"***********Updating at path: {path}...")
        self.zk.ensure_path(path)
        self.zk.set(path, value)


    def findLeader(self):
        for node in self.electionNodeList:
            try:
                self.zk.exists(node)
                val = self.zk.get(node, watch=self.ElectionWatcher)[0]
                if val == b'leader':
                    print(f"CLIENT {self.ID}, LEADER NODE: {node}")
                    return node
            except:
                print("All Servers Down!")
                return None

    def getElectionServers(self):
        self.electionNodeList = self.zk.get_children(self.electionPrefix)
        index = 0
        for node in self.electionNodeList:
            newNode = self.electionPrefix + node
            self.electionNodeList[index] = newNode
            index += 1

    #### WATCHER FUNCTIONS ####
    def ElectionWatcher(self, event):
        print("**************Deleted Event**************")
        self.leaderNode = self.findLeader()
        print(f"Updated client leader node: {self.leaderNode}")
        # on a deleted event, "election" occurs by querying the election servers again

