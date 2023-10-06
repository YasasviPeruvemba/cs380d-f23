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
import threading

baseAddr = "http://localhost:"
baseServerPort = 9000

class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
        pass


def put_helper(func, server, key, value):
    count = 0
    resp = ""
    while count < 20:
        try:
            # resp = "{}".format(server)
            resp = func(key, value) + "\n"
            return resp
        except Exception as e:
            resp = "Failed {} times:{}:{}\n".format(count, server, str(e))
            count += 1
            time.sleep(0.06 * count)

    return resp[:-1]


class FrontendRPCServer:
    # TODO: You need to implement details for these functions.
    def __init__(self):
        self.locked_keys = defaultdict(Lock)
        self.kvsServers  = dict()
        self.executor = ThreadPoolExecutor(16)
        self.heartbeat_thread = threading.Thread(target=self.hearbeat)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()

    def hearbeat(self):
        while True:
            time.sleep(1)
            faulty_servers = []
            for id, server in self.kvsServers.items():
                count = 0
                success = False
                while count < 10:
                    try:
                        server.ping()
                        success = True
                        count = 10
                    except:
                        time.sleep(0.05 * count)
                        count += 1
                if not success:
                    faulty_servers.append(id)
            for serverId in faulty_servers:
                self.kvsServers.pop(serverId)

    ## put: This function routes requests from clients to proper
    ## servers that are responsible for inserting a new key-value
    ## pair or updating an existing one.
    def put(self, key, value):
        # Lock the key so that nobody reads while it is updated
        if self.locked_keys.get(key, None) is None:
            self.locked_keys[key] = Lock()

        self.locked_keys[key].acquire()

        responses = [self.executor.submit(put_helper,
                                          func=self.kvsServers[server].put,
                                          server=server,
                                          key=key,
                                          value=value) for server, _ in self.kvsServers.items()]
        concurrent.futures.wait(responses, return_when=concurrent.futures.ALL_COMPLETED)
        
        # release the lock
        self.locked_keys[key].release()
        
        resp = ""
        for response in responses:
            res = str(response.result())
            resp += res + "\n"

        return resp + "{}\n".format(time.time_ns())

    ## get: This function routes requests from clients to proper
    ## servers that are responsible for getting the value
    ## associated with the given key.
    def get(self, key):
        # Making sure this key is not being updates currently
        if self.locked_keys.get(key, None) is not None:
            while self.locked_keys[key].locked():
                time.sleep(0.0001)
        
        res = ""
        # while we know some server is alive, send the value
        while len(self.kvsServers.keys()) > 0:
            lst = list(self.kvsServers.keys())
            serverId = lst[random.randint(0, len(lst) - 1)]
            try:
                get_val = self.kvsServers[serverId].get(key)
                # res += str(get_val) + "\n{}\n".format(time.time_ns())
                # return res
                return get_val
            except Exception as e:
                res += "Detected failure for server : {}\n{} | {}\n".format(serverId, time.time_ns(), str(e))
        
        # return "No active server.\n{}\n".format(res, time.time_ns())
        return "ERR_NOEXIST"

    ## printKVPairs: This function routes requests to servers
    ## matched with the given serverIds.
    def printKVPairs(self, serverId):
        count = 0
        while count < 3:
            try:
                resp = self.kvsServers[serverId].printKVPairs()
                return resp
            except:
                resp = "Server {} is dead after retrying 3 times.".format(serverId)
                count += 1
        return resp

    ## addServer: This function registers a new server with the
    ## serverId to the cluster membership.
    def addServer(self, serverId):
        new_server = xmlrpc.client.ServerProxy(baseAddr + str(baseServerPort + serverId))

        if len(self.kvsServers) == 0:
            self.kvsServers[serverId] = new_server
            return "No active server to copy from"

        # Copy data from any other server
        for server, _ in self.kvsServers.items():
            try:
                kvPairs = self.printKVPairs(server)
            except Exception as e:
                continue
            try:
                new_server.copy(kvPairs)
                self.kvsServers[serverId] = new_server
                return "Copy Succeeded for {}.".format(serverId)
            except Exception as e:
                return "New Server {} Died.".format(serverId)

        self.kvsServers[serverId] = new_server

        return "Copy Failed for {}.".format(serverId)

    ## listServer: This function prints out a list of servers that
    ## are currently active/alive inside the cluster.
    def listServer(self):
        serverList = []
        for serverId, _ in self.kvsServers.items():
            serverList.append(str(serverId))
        if len(serverList) == 0:
            return "ERR_NOSERVERS"
        return ",".join(serverList)

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
