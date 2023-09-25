import argparse
import xmlrpc.client
import xmlrpc.server
import collections
import time

serverId = 0
basePort = 9000

class KVSRPCServer:
    def __init__(self):
        self.kvs = collections.defaultdict(int)
    # TODO: You need to implement details for these functions.

    ## put: Insert a new-key-value pair or updates an existing
    ## one with new one if the same key already exists.
    def put(self, key, value):
        self.kvs[key] = int(value)
        return "[Server " + str(serverId) + "] Receive a put request: " + "Key = " + str(key) + ", Val = " + str(value)

    ## get: Get the value associated with the given key.
    def get(self, key):
        return self.kvs[key]
        # return "[Server " + str(serverId) + "] Receive a get request: " + "Key = " + str(key)

    ## printKVPairs: Print all the key-value pairs at this server.
    def printKVPairs(self):
        res = ""
        for key, value in self.kvs.items():
            res += "{}:{}\n".format(key, value)
        if len(res) > 0:
            return res[:-1]
        return res
    
    def copy(self, kvPairs):
        kvs = kvPairs.split("\n")[:-1]
        if len(kvs) > 0:
            for kv in kvs:
                if len(kv) > 0:
                    key, value = kv.split(":")
                    self.kvs[key] = value
        return kvPairs


    ## shutdownServer: Terminate the server itself normally.
    def shutdownServer(self):
        return "[Server " + str(serverId) + "] Receive a request for a normal shutdown"

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = '''To be added.''')

    parser.add_argument('-i', '--id', nargs=1, type=int, metavar='I',
                        help='Server id (required)', dest='serverId', required=True)

    args = parser.parse_args()

    serverId = args.serverId[0]

    server = xmlrpc.server.SimpleXMLRPCServer(("localhost", basePort + serverId))
    server.register_instance(KVSRPCServer())

    server.serve_forever()
