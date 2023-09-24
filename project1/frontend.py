import xmlrpc.client
import xmlrpc.server
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from collections import defaultdict
import concurrent.futures

import time
import random

baseAddr = "http://localhost:"
baseServerPort = 9000

class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
        pass


def put_helper(func, server, key, value):
    try:
        # resp = "{}".format(server)
        resp = func(key, value)
    except Exception as e:
        resp = "Failed:{}".format(server)
    return resp


class FrontendRPCServer:
    # TODO: You need to implement details for these functions.
    def __init__(self):
        self.locked_keys = set()
        self.kvsServers  = dict()
        self.executor = ThreadPoolExecutor(16)

    ## put: This function routes requests from clients to proper
    ## servers that are responsible for inserting a new key-value
    ## pair or updating an existing one.
    def put(self, key, value):
        # Lock the key so that nobody reads while it is updated
        self.locked_keys.add(key)

        clk1 = time.time_ns()
        responses = [self.executor.submit(put_helper,
                                          func=self.kvsServers[server].put,
                                          server=server,
                                          key=key,
                                          value=value) for server, _ in self.kvsServers.items()]
        concurrent.futures.wait(responses, return_when=concurrent.futures.ALL_COMPLETED)
        clk2 = time.time_ns()
        
        # release the lock
        self.locked_keys.remove(key)
        
        resp = ""
        faulty_servers = []
        for response in responses:
            res = str(response.result())
            if "Failed" in res:
                faulty_servers.append(int(res.split(":")[1]))
            resp += str(response.result()) + "\n"

        for fault in faulty_servers:
            self.kvsServers.pop(fault, None)
        
        return resp + "Time Taken : {}ns".format(clk2 - clk1)

    ## get: This function routes requests from clients to proper
    ## servers that are responsible for getting the value
    ## associated with the given key.
    def get(self, key):
        # Making sure this key is not being updates currently
        while key in self.locked_keys:
            time.sleep(0.001)
        
        # while we know some server is alive, send the value
        while len(self.kvsServers.keys()) > 0:
            lst = list(self.kvsServers.keys())
            serverId = lst[random.randint(0, len(lst) - 1)]
            try:
                res = self.kvsServers[serverId].get(key)
                return res
            except:
                print("Detected failure for server : {}".format(serverId))
                self.kvsServers.pop(serverId, None)
        
        return "None"

    ## printKVPairs: This function routes requests to servers
    ## matched with the given serverIds.
    def printKVPairs(self, serverId):
        clk1 = time.time_ns()
        try:
            resp = self.kvsServers[serverId].printKVPairs()
            clk2 = time.time_ns()
            resp += "\nTime Taken : {}ns".format(clk2 - clk1)
        except:
            clk2 = time.time_ns()
            resp = "Server {} is dead. Time Taken : {}".format(serverId, clk2 - clk1)
            self.kvsServers.pop(serverId, None)
        return resp

    ## addServer: This function registers a new server with the
    ## serverId to the cluster membership.
    def addServer(self, serverId):
        new_server = xmlrpc.client.ServerProxy(baseAddr + str(baseServerPort + serverId))
        faulty_servers = []

        if len(self.kvsServers) == 0:
            self.kvsServers[serverId]=new_server
            return "No active server to copy from"

        clk1 = time.time_ns()
        # Copy data from any other server
        for server, _ in self.kvsServers.items():
            try:
                kvPairs = self.printKVPairs(server)
            except Exception as e:
                faulty_servers.append(server)
                continue
            # TODO: handle serverId death
            try:
                new_server.copy(kvPairs)
                self.kvsServers[serverId] = new_server
                clk2 = time.time_ns()
                return "Copy Succeeded. Time Taken : {}ns".format(clk2 - clk1)
            except Exception as e:
                clk2 = time.time_ns()
                return "New Server Died. Time Taken : {}ns".format(clk2 - clk1)
            
        clk2 = time.time_ns()

        self.kvsServers[serverId] = new_server

        for fault in faulty_servers:
            self.kvsServers.pop(fault, None)

        return "Copy Failed. Time Taken : {}ns".format(clk2 - clk1)

    ## listServer: This function prints out a list of servers that
    ## are currently active/alive inside the cluster.
    def listServer(self):
        serverList = []
        for serverId, _ in self.kvsServers.items():
            serverList.append(serverId)
        return serverList

    ## shutdownServer: This function routes the shutdown request to
    ## a server matched with the specified serverId to let the corresponding
    ## server terminate normally.
    def shutdownServer(self, serverId):
        result = self.kvsServers[serverId].shutdownServer()
        self.kvsServers.pop(serverId, None)
        return result
    

server = SimpleThreadedXMLRPCServer(("localhost", 8001))
server.register_instance(FrontendRPCServer())

server.serve_forever()
