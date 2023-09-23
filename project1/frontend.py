import xmlrpc.client
import xmlrpc.server
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
import time
import random

baseAddr = "http://localhost:"
baseServerPort = 9000

class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
        pass

class FrontendRPCServer:
    # TODO: You need to implement details for these functions.
    def __init__(self):
        self.locked_keys = set()
        self.kvsServers  = dict()

    ## put: This function routes requests from clients to proper
    ## servers that are responsible for inserting a new key-value
    ## pair or updating an existing one.
    def put(self, key, value) -> bool:
        # Lock the key so that nobody reads while it is updated
        self.locked_keys.add(key)
        resp = False

        for server in self.kvsServers.keys():
            try:
                resp = resp or self.kvsServers[server].put(key, value)
            except Exception as e:
                print("Detected failure for server : {}".format(server))
                self.kvsServers.pop(server)
        
        # release the lock
        self.locked_keys.remove(key)
        
        return resp

    ## get: This function routes requests from clients to proper
    ## servers that are responsible for getting the value
    ## associated with the given key.
    def get(self, key):
        # Making sure this key is not being updates currently
        while key in self.locked_keys:
            time.sleep(0.001)
        
        # while we know some server is alive, send the value
        while len(self.kvsServers.keys()) > 0:
            serverId = self.kvsServers.keys()[random.randint(0, len(self.kvsServers.keys()))]
            try:
                res = self.kvsServers[serverId].get(key)
                return res
            except:
                print("Detected failure for server : {}".format(serverId))
                self.kvsServers.pop(serverId)
        
        return "None"

    ## printKVPairs: This function routes requests to servers
    ## matched with the given serverIds.
    def printKVPairs(self, serverId):
        return self.kvsServers[serverId].printKVPairs()

    ## addServer: This function registers a new server with the
    ## serverId to the cluster membership.
    def addServer(self, serverId):
        self.kvsServers[serverId] = xmlrpc.client.ServerProxy(baseAddr + str(baseServerPort + serverId))
        return "Success"

    ## listServer: This function prints out a list of servers that
    ## are currently active/alive inside the cluster.
    def listServer(self):
        serverList = []
        for serverId, rpcHandle in self.kvsServers.items():
            serverList.append(serverId)
        return serverList

    ## shutdownServer: This function routes the shutdown request to
    ## a server matched with the specified serverId to let the corresponding
    ## server terminate normally.
    def shutdownServer(self, serverId):
        result = self.kvsServers[serverId].shutdownServer()
        self.kvsServers.pop(serverId)
        return result

    # def 
    

server = SimpleThreadedXMLRPCServer(("localhost", 8001))
server.register_instance(FrontendRPCServer())

server.serve_forever()
