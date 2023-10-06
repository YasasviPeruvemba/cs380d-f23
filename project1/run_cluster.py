import argparse
import os
import subprocess

import random
import xmlrpc.client

import time

import concurrent.futures

from multiprocessing import Pool

from shared import util

baseAddr = "http://localhost:"
baseClientPort = 7000
baseFrontendPort = 8001
baseServerPort = 9000

clientUID = 0
serverUID = 0

frontend = None
clientList = dict()

def add_nodes(k8s_client, k8s_apps_client, node_type, num_nodes, prefix=None):
    global clientUID
    global serverUID

    result = ""
    for i in range(0, num_nodes):
        if node_type == 'server':
            server_spec = util.load_yaml('yaml/pods/server-pod.yml', prefix)
            env = server_spec['spec']['containers'][0]['env']
            util.replace_yaml_val(env, 'SERVER_ID', str(serverUID))
            server_spec['metadata']['name'] = 'server-pod-%d' % serverUID
            server_spec['metadata']['labels']['role'] = 'server-%d' % serverUID
            k8s_client.create_namespaced_pod(namespace=util.NAMESPACE, body=server_spec)
            util.check_wait_pod_status(k8s_client, 'role=server-%d' % serverUID, 'Running')
            result += frontend.addServer(serverUID) + "\n"
            serverUID += 1
        elif node_type == 'client':
            client_spec = util.load_yaml('yaml/pods/client-pod.yml', prefix)
            env = client_spec['spec']['containers'][0]['env']
            util.replace_yaml_val(env, 'CLIENT_ID', str(clientUID))
            client_spec['metadata']['name'] = 'client-pod-%d' % clientUID
            client_spec['metadata']['labels']['role'] = 'client-%d' % clientUID
            k8s_client.create_namespaced_pod(namespace=util.NAMESPACE, body=client_spec)
            util.check_wait_pod_status(k8s_client, 'role=client-%d' % clientUID, 'Running')
            result += "Created client {}".format(clientUID) + "\n"
            clientList[clientUID] = xmlrpc.client.ServerProxy(baseAddr + str(baseClientPort + clientUID))
            clientUID += 1
        else:
            result += "Unknown pod type"
            # print("Unknown pod type")
            # exit()
    return result[:-1]

def remove_node(k8s_client, k8s_apps_client, node_type, node_id):
    name = node_type + '-pod-%d' % node_id
    selector = 'role=' + node_type + '-%d' % node_id
    k8s_client.delete_namespaced_pod(name, namespace=util.NAMESPACE)
    util.check_wait_pod_status(k8s_client, selector, 'Terminating')

def addClient(k8s_client, k8s_apps_client, prefix):
    result = add_nodes(k8s_client, k8s_apps_client, 'client', 1, prefix)
    print(result)

def addServer(k8s_client, k8s_apps_client, prefix):
    result = add_nodes(k8s_client, k8s_apps_client, 'server', 1, prefix)
    print(result)

def listServer():
    result = frontend.listServer()
    print(result)

def killServer(k8s_client, k8s_apps_client, serverId):
    remove_node(k8s_client, k8s_apps_client, 'server', serverId)
    print("Killed server {}".format(serverId))

def shutdownServer(k8s_client, k8s_apps_client, serverId):
    result = frontend.shutdownServer(serverId)
    remove_node(k8s_client, k8s_apps_client, 'server', serverId)
    print(result)

def put(key, value):
    client = random.randint(1, 100000) % len(clientList)
    try:
        result = clientList[client].put(key, value)
    except Exception as e:
        result = "Exception in put " + str(e)
    # print(result)
    return result + "Client = " + str(client) + "\n"

def get(key, value = None):
    client = random.randint(1, 100000) % len(clientList)
    try:
        result = clientList[client].get(key)
    except Exception as e:
        result = "Exception in get " + str(e)
    # print(result)
    return result + "Client = " + str(client) + "\n"

def printKVPairs(serverId):
    result = frontend.printKVPairs(serverId)
    print(result)

def loadDataset(thread_id, keys, load_vals, num_threads):
    start_idx = int((len(keys) / num_threads) * thread_id)
    end_idx = int(start_idx + (int((len(keys) / num_threads))))

    for idx in range(start_idx, end_idx):
        result = clientList[thread_id].put(keys[idx], load_vals[idx])

def runWorkload(k8s_client, k8s_apps_client, prefix, thread_id,
                keys, load_vals, run_vals, num_threads, num_requests,
                put_ratio, test_consistency, crash_server, add_server, remove_server):
    request_count = 0
    start_idx = int((len(keys) / num_threads) * thread_id)
    end_idx = int(start_idx + (int((len(keys) / num_threads))))

    if test_consistency == 1:
        while num_requests > request_count:
            idx = random.randint(start_idx, end_idx - 1)
            if thread_id == 0 and request_count == int(num_requests / 2):
                print("Here : {} {} {}".format(crash_server, add_server, remove_server))
                serverList = frontend.listServer()
                serverToKill = int(serverList.split(',')[-1])
                if crash_server == 1:
                    killServer(k8s_client, k8s_apps_client, serverToKill)
                elif add_server == 1:
                    addServer(k8s_client, k8s_apps_client, prefix)
                elif remove_server == 1:
                    shutdownServer(k8s_client, k8s_apps_client, serverToKill)
            newval = random.randint(0, 1000000)
            res = clientList[thread_id].put(keys[idx], newval)
            result = clientList[thread_id].get(keys[idx])
            if "Failed" not in res:
                result = result.split(':')
                if int(result[0]) != keys[idx] or int(result[1]) != newval:
                    print("[Error] request = (%d, %d), return = (%d, %d) on thread : %d" % (keys[idx], newval, int(result[0]), int(result[1]), thread_id))
                    return
            request_count += 1
    else:
        print("Thread {} is here..".format(thread_id))
        optype = []
        for i in range(0, 100):
            if (i % 100) < put_ratio:
                optype.append("Put")
            else:
                optype.append("Get")
        random.seed(42)
        random.shuffle(optype)

        while num_requests > request_count:
            for idx in range(start_idx, end_idx):
                if request_count == num_requests:
                    break
                if optype[idx % 100] == "Put":
                    result = clientList[thread_id].put(keys[idx], run_vals[idx])
                elif optype[idx % 100] == "Get":
                    result = clientList[thread_id].get(keys[idx])
                    result = result.split(':')
                    if int(result[0]) != keys[idx] or int(result[1]) != load_vals[idx]:
                        print("[Error] request = (%d, %d), return = (%d, %d) in thread %d" % (keys[idx], load_vals[idx], int(result[0]), int(result[1]), thread_id))
                        return
                else:
                    print("[Error] unknown operation type")
                    return
                request_count += 1

def testKVS(k8s_client, k8s_apps_client, prefix, num_keys, num_threads,
            num_requests, put_ratio, test_consistency=0, crash_server=0,
            add_server=0, remove_server=0):
    serverList = frontend.listServer()
    serverList = serverList.split(',')
    if len(serverList) < 1:
        print("[Error] Servers do not exist")
        return

    if len(clientList) < num_threads:
        print("[Warning] Clients should exist more than # of threads")
        print("[Warning] Add %d more clients" % (num_threads - len(clientList)))
        for i in range(len(clientList), num_threads):
            addClient(k8s_client, k8s_apps_client, prefix)

    keys = list(range(0, num_keys))
    load_vals = list(range(0, num_keys))
    run_vals = list(range(num_keys, num_keys * 2))

    random.shuffle(keys)
    random.shuffle(load_vals)
    random.shuffle(run_vals)

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=num_threads)
    start = time.time()
    for thread_id in range(0, num_threads):
        pool.submit(loadDataset, thread_id, keys, load_vals, num_threads)
    pool.shutdown(wait=True)
    end = time.time()
    print("Load throughput = " + str(round(num_keys/(end - start), 1)) + "ops/sec")

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=num_threads)
    start = time.time()
    for thread_id in range(0, num_threads):
        pool.submit(runWorkload, k8s_client, k8s_apps_client, prefix,
                    thread_id, keys, load_vals, run_vals,
                    num_threads, int(num_requests / num_threads), put_ratio,
                    test_consistency, crash_server, add_server, remove_server)
    pool.shutdown(wait=True)
    end = time.time()
    print("Run throughput = " + str(round(num_requests/(end - start), 1)) + "ops/sec")

def init_cluster(k8s_client, k8s_apps_client, num_client, num_server, ssh_key, prefix):
    global frontend

    print('Creating a frontend pod...')
    frontend_spec = util.load_yaml('yaml/pods/frontend-pod.yml', prefix)
    env = frontend_spec['spec']['containers'][0]['env']
    k8s_client.create_namespaced_pod(namespace=util.NAMESPACE, body=frontend_spec)
    util.check_wait_pod_status(k8s_client, 'role=frontend', 'Running')
    frontend = xmlrpc.client.ServerProxy(baseAddr + str(baseFrontendPort))

    print('Creating server pods...')
    result = add_nodes(k8s_client, k8s_apps_client, 'server', num_server, prefix)
    print(result)

    print('Creating client pods...')
    result = add_nodes(k8s_client, k8s_apps_client, 'client', num_client, prefix)
    print(result)
    
def helper_func(key):
    if random.random() < 0.1:
        return put(key, random.randint(100, 200))
    return get(key)

def event_trigger(k8s_client, k8s_apps_client, prefix):
    terminate = False
    while terminate != True:
        cmd = input("Enter a command: ")
        args = cmd.split(':')

        if args[0] == 'addClient':
            addClient(k8s_client, k8s_apps_client, prefix)
        elif args[0] == 'addServer':
            addServer(k8s_client, k8s_apps_client, prefix)
        elif args[0] == 'listServer':
            listServer()
        elif args[0] == 'killServer':
            serverId = int(args[1])
            killServer(k8s_client, k8s_apps_client, serverId)
        elif args[0] == 'shutdownServer':
            serverId = int(args[1])
            shutdownServer(k8s_client, k8s_apps_client, serverId)
        elif args[0] == 'put':
            key = int(args[1])
            value = int(args[2])
            put(key, value)
        elif args[0] == 'get':
            key = int(args[1])
            get(key)
        elif args[0] == 'printKVPairs':
            serverId = int(args[1])
            printKVPairs(serverId)
        elif args[0] == 'terminate':
            terminate = True
        elif args[0] == "testConcurrency":
            key = 10
            put(key, 150)
            with open("dumped_responses_serial.log", "w") as f:
                for _ in range(5000):
                    resp = helper_func(key)
                    f.write(resp + "\n")

            print("Serial processing is done.")

            with open("dumped_responses_parallel.log", "w") as f:
                with Pool(16) as p:
                    x = p.map(helper_func, [key for _ in range(5000)])
                    f.write("\n".join(x))

            print("Parallel processing is done.")
        elif args[0] == 'testKVS':
            num_keys = int(args[1])
            num_threads = int(args[2])
            num_requests = int(args[3])
            put_ratio = int(args[4])
            test_consistency = int(args[5])
            crash_server = int(args[6])
            add_server = int(args[7])
            remove_server = int(args[8])
            testKVS(k8s_client, k8s_apps_client, prefix, num_keys, num_threads,
                    num_requests, put_ratio, test_consistency, crash_server,
                    add_server, remove_server)
        else:
            print("Unknown command")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='''Create a KVS cluster using Kubernetes
                                    and Kubespray. If no SSH key is specified, we use the
                                    default SSH key (~/.ssh/id_rsa), and we expect that
                                    the corresponding public key has the same path and ends
                                    in .pub. If no configuration file base is specified, we
                                    use the default ($KVS_HOME/conf/kvs-base.yml).''')

    if 'KVS_HOME' not in os.environ:
        os.environ['KVS_HOME'] = "/home/" + os.environ['USER'] + "/projects/cs380d-f23/project1/"

    parser.add_argument('-c', '--client', nargs=1, type=int, metavar='C',
                        help='The number of client nodes to start with ' +
                        '(required)', dest='client', required=True)
    parser.add_argument('-s', '--server', nargs=1, type=int, metavar='S',
                        help='The number of server nodes to start with ' +
                        '(required)', dest='server', required=True)
    parser.add_argument('--ssh-key', nargs='?', type=str,
                        help='The SSH key used to configure and connect to ' +
                        'each node (optional)', dest='sshkey',
                        default=os.path.join(os.environ['HOME'], '.ssh/id_rsa'))

    args = parser.parse_args()

    prefix = os.environ['KVS_HOME']

    k8s_client, k8s_apps_client = util.init_k8s()

    init_cluster(k8s_client, k8s_apps_client, args.client[0], args.server[0], args.sshkey, prefix)

    event_trigger(k8s_client, k8s_apps_client, prefix)