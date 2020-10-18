from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging
from Server import Server
from Client import Client

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

if __name__ == "__main__":
    zk = KazooClient(hosts = '127.0.0.1:2181')
    zk.start()
    zk.add_listener(myListener)

    server1 = Server(zk)
    server2 = Server(zk)
    server3 = Server(zk)

    client = Client(zk)
    client.Add_Update("hello", b'test')
    val = client.Read("hello")
    client.Add_Update("yo", b"test2")

    if val:
        logging.critical("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        logging.critical(val)
    val = client.Read("yo")
    if val:
        logging.critical("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        logging.critical(val)


    # todo
    # add watchers to subnodes of the leader node
    # add watchers to check on a new /election/ dead node
    # FART

    del server1
    del server2
    del server3

    zk.stop()