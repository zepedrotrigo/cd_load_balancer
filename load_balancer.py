# coding: utf-8

import socket
import selectors
import signal
import logging
import argparse
import time

# configure logger output format
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('Load Balancer')


# used to stop the infinity loop
done = False

sel = selectors.DefaultSelector()

policy = None
mapper = None

# implements a graceful shutdown
def graceful_shutdown(signalNumber, frame):  
    logger.debug('Graceful Shutdown...')
    global done
    done = True


# n to 1 policy
class N2One:
    def __init__(self, servers):
        self.servers = servers  

    def select_server(self):
        return self.servers[0]

    def update(self, *arg):
        pass


# round robin policy
class RoundRobin:
    def __init__(self, servers):
        self.servers = servers
        self.counter = -1

    def select_server(self):
        self.counter += 1
        return self.servers[self.counter % len(self.servers)]
    
    def update(self, *arg): # no round robin nao preciso
        pass


# least connections policy
class LeastConnections:
    def __init__(self, servers):
        self.servers = servers
        self.connections = {sv : 0 for sv in self.servers}

    def select_server(self):
        return min(self.connections, key=self.connections.get)

    def update(self, *arg):
        add = arg[0]
        server = arg[1]

        if add:
            self.connections[server] += 1
        else:
            self.connections[server] -= 1

# least response time
class LeastResponseTime:
    def __init__(self, servers):
        self.servers = servers
        self.response_times = {sv:0 for sv in self.servers}
        self.temp = {}

    def select_server(self):
        return min(self.response_times, key=self.response_times.get)

    def update(self, *arg): # fazer um update da informacao no fim
        add = arg[0]
        server = arg[1]
        client_socket = arg[2]

        if add:
            self.temp[server, client_socket] = [0, time.time()]
        else:
            initial_time = self.temp[server, client_socket]
            self.temp[server, client_socket] = [1, time.time() - initial_time]

        aux = {}

        for k,v in self.temp.items():
            server = k[0]
            calculated_difference, stored_time = v

            if calculated_difference and server not in aux:
                aux[server] = [stored_time]
            elif calculated_difference:
                aux[server].append(stored_time)

        for server, lst in aux.items():
            self.response_times[server] = sum(lst) / len(lst)

        aux.clear()

class Cache:
    def __init__(self):
        self.cache = {}
        
    def check(self, k):
        if k in self.cache.keys():
            return self.cache[k] 
        else:
            #print("Not in cache!")
            return None
        
    def update(self, k, v):
        self.cache[k] = v
    
    
class SocketMapper:
    def __init__(self, policy):
        self.policy = policy
        self.map = {}

    def add(self, client_sock, upstream_server):
        client_sock.setblocking(False)
        sel.register(client_sock, selectors.EVENT_READ, read)
        upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        upstream_sock.connect(upstream_server)
        upstream_sock.setblocking(False)
        sel.register(upstream_sock, selectors.EVENT_READ, read)
        logger.debug("Proxying to %s %s", *upstream_server)
        self.map[client_sock] =  upstream_sock
        self.policy.update(1, upstream_server, client_sock) # 0 = remove, 1 = add

    def delete(self, sock):
        try:
            self.map.pop(sock)
            sel.unregister(sock)
            upstream_server = self.connections.pop(sock)
            self.policy.update(0, upstream_server, sock) # 0 = remove, 1 = add
            sock.close() 
        except KeyError:
            pass

    def get_sock(self, sock):
        for client, upstream in self.map.items():
            if upstream == sock:
                return client
            if client == sock:
                return upstream
        return None
    
    def get_upstream_sock(self, sock):
        return self.map.get(sock)

    def get_all_socks(self):
        """ Flatten all sockets into a list"""
        return list(sum(self.map.items(), ())) 

def accept(sock, mask):
    client, addr = sock.accept()
    logger.debug("Accepted connection %s %s", *addr)
    server = policy.select_server() # saved this in a variable
    mapper.add(client, server)

def read(conn,mask):
    data = conn.recv(4096)
    if len(data) == 0: # No messages in socket, we can close down the socket
        mapper.delete(conn)
    else:
        mapper.get_sock(conn).send(data)


def main(addr, servers):
    global policy
    global mapper

    # register handler for interruption 
    # it stops the infinite loop gracefully
    signal.signal(signal.SIGINT, graceful_shutdown)

    policy = N2One(servers)
    mapper = SocketMapper(policy)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(addr)
    sock.listen()
    sock.setblocking(False)

    sel.register(sock, selectors.EVENT_READ, accept)

    try:
        logger.debug("Listening on %s %s", *addr)
        while not done:
            events = sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
                
    except Exception as err:
        logger.error(err)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pi HTTP server')
    parser.add_argument('-p', dest='port', type=int, help='load balancer port', default=8080)
    parser.add_argument('-s', dest='servers', nargs='+', type=int, help='list of servers ports')
    args = parser.parse_args()
    
    servers = [('localhost', p) for p in args.servers]
    
    main(('127.0.0.1', args.port), servers)
