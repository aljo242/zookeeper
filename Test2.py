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
    print("Putting value: 'test' to key: 'hello'...")
    client.Add_Update("hello", b'test')
    print("Reading value from key: 'hello'...")
    val = client.Read("hello")
    print(f"\tval = {val}")
    print("Putting value: 'test2' to key: 'yo'...")
    client.Add_Update("yo", b"test2")
    print("Reading value from key: 'yo'...")
    val = client.Read("yo")
    print(f"\tval = {val}")

    print("Putting value: 'test2new' to key: 'yo'...")
    client.Add_Update("yo", b"test2new")
    print("Reading value from key: 'yo'...")
    val = client.Read("yo")
    print(f"\tval = {val}")

    print("deleting the leader server...")
    del server1

    print("Putting value: 'test2newnew' to key: 'yo'...")
    client.Add_Update("yo", b"test2newnew")
    print("Reading value from key: 'yo'...")
    val = client.Read("yo")
    print(f"\tval = {val}")

    print("deleting the two remaining servers")
    del server2
    del server3

    zk.stop()