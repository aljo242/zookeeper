from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging
from Server import Server, cleanUp
from Client import Client
import time

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

def printVal(val):
    print(f"\tRecieved val = {str(val)}")



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
    printVal(val)
    print("Putting value: 'test2' to key: 'yo'...")
    client.Add_Update("yo", b"test2")
    print("Reading value from key: 'yo'...")
    val = client.Read("yo")
    printVal(val)

    print("Putting value: 'test2new' to key: 'yo'...")
    client.Add_Update("yo", b"test2new")
    print("Reading value from key: 'yo'...")
    val = client.Read("yo")
    printVal(val)

    print("Putting value: 'test2newnew' to key: 'yo'...")
    client.Add_Update("yo", b"test2newnew")
    print("Reading value from key: 'yo'...")
    val = client.Read("yo")
    printVal(val)

    print("Reading value from key: 'hello'...")
    val = client.Read("hello")
    printVal(val)

    print("Disconnecting All servers")
    time.sleep(1)

    server3.Disconnect()
    server2.Disconnect()
    server1.Disconnect()

    cleanUp(zk)
    zk.stop()
    zk.close()