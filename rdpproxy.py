#!/usr/bin/env python3
import socket
import select
import sqlite3
import time
import threading
import os
import sys
import json

host = ''
server_port = 3389
info_port = 3391

class InfoAPI(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self, daemon=True, name="API")
        self.info_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.info_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1, )
        self.info_sock.bind((host, info_port))
        self.info_sock.listen(5)

    def run(self):
        while True:
            sock, addr = self.info_sock.accept()
            info = {}
            info['proxy'] = self.get_proxy_list()
            sock.send(bytes(json.dumps(info), "utf-8"))
            sock.close()

    def get_proxy_list(self):
        proxies = []
        for th in threading.enumerate():
            if th.name == "worker":
                proxy = {}
                proxy['client'] = th.get_client_address()
                proxy['server'] = th.get_server_address()
                proxies.append(proxy)
        return proxies

    def close_socket(self):
        self.info_sock.close()

class ProxyTCP(threading.Thread):
    def __init__(self, client):
        threading.Thread.__init__(self, daemon=True, name="worker")
        self.client_sock, (self.client_address, self.client_port) = client
        self.pool = Pool()
        self.server_sock, (self.server_address, self.server_port) = self.pool.next(self.client_address)
        self.bufsize = 4096

    def run(self):
        readfds = set([self.client_sock])
        if self.server_sock != None:
            readfds.add(self.server_sock)
        while True:
            rready, wready, xready = select.select(readfds, [], [])
            for sock in rready:
                try:
                    msg = sock.recv(self.bufsize)
                    if len(msg) == 0:
                        break
                    peer_sock = self.peer(sock)
                    if peer_sock != None:
                        peer_sock.send(msg)
                    else:
                        if msg[0:1] == b'\x03':
                            # sendback disconnect request
                            tpktHeader = b'\x03\x00\x00\x0b'
                            x224Ccf = b'\x06\x80\x00\x00\x00\x00\x00'
                            res = tpktHeader + x224Ccf
                            sock.send(res)
                        else:
                            break
                except Exception as e:
                    break
            else:
                continue
            break
        self.close_sockets()

    def peer(self, sock):
        if sock is self.client_sock:
            return self.server_sock
        else:
            return self.client_sock

    def get_server_address(self):
        return self.server_address

    def get_client_address(self):
        return self.client_address

    def close_sockets(self):
        server_address = self.server_address
        self.client_address = None
        self.server_address = None
        try:
            self.client_sock.close()
        except Exception as e:
            pass
        try:
            self.server_sock.close()
        except Exception as e:
            pass
        self.pool.release(server_address)

class Pool():
    database = 'db.sqlite3'

    def __init__(self, server_port = 3389):
        self.server_port = server_port
        db = sqlite3.connect(self.database)
        db.execute('''create table if not exists pool (server text primary key, weight int, client text, wait_start datetime)''')
        db.execute('''create table if not exists static (client text primary key, server text)''')
        db.close()

    def next(self, client_address):
        server_address = self.get_server(client_address)
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server_sock.connect((server_address, self.server_port))
            self.register_client(server_address, client_address)
            return server_sock, (server_address, self.server_port)
        except Exception as e:
            pass
            return None, (None, None)

    def release(self, server_address):
        self.register_client(server_address, '')

    def get_server(self, client_address):
        db = sqlite3.connect(self.database)
        cur = db.cursor()

        cur.execute("select server from static where client=?", (client_address,))
        one = cur.fetchone()
        if one == None:
            cur.execute("select server from pool where client=?", (client_address,))
            one = cur.fetchone()
            if one == None:
                cur.execute("select server from pool where weight<>0 and (client='' or client is null) order by weight desc")
                one = cur.fetchone()

        cur.close()
        db.close()

        if one == None:
            return None
        return one[0]

    def register_client(self, server_address, client_address):
        db = sqlite3.connect(self.database)
        cur = db.cursor()

        cur.execute("select * from pool where client=? and server=? and (wait_start is null or rtrim(wait_start) = '')", (client_address, server_address))
        one = cur.fetchone()
        if one == None:
            cur.execute("update pool set client=?, wait_start='' where server=?", (client_address, server_address))
            db.commit()

        cur.close()
        db.close()

    def clear_invalid_client(self, client_addresses):
        db = sqlite3.connect(self.database)
        cur = db.cursor()

        query = f"update pool set wait_start=julianday('now') where client is not null and rtrim(client) <> '' and (wait_start is null or rtrim(wait_start) = '') and client not in ({','.join(['?']*len(client_addresses))})"
        cur.execute(query, client_addresses)
        query = f"update pool set client='', wait_start='' where wait_start is not null and rtrim(wait_start) <> '' and wait_start < julianday('now', '-10 minutes')"
        cur.execute(query)
        db.commit()

        cur.close()
        db.close()

class Cleanup(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self, daemon=True, name="cleanup")
        self.pool = Pool()

    def run(self):
        while True:
            client_addresses = []
            for th in threading.enumerate():
                if th.name == "worker":
                    self.pool.register_client(th.get_server_address(), th.get_client_address())
                    client_addresses.append(th.get_client_address())
            self.pool.clear_invalid_client(client_addresses)
            time.sleep(3)

def main():
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1, )
    #udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    th1 = Cleanup()
    th1.start()
    th2 = InfoAPI()
    th2.start()

    try:
        tcp.bind((host, server_port))
        tcp.listen(5)
        while True:
            th = ProxyTCP(tcp.accept())
            th.start()
    except Exception as e:
        print(e)
    finally:
        return

def daemonize():
    pid = os.fork()

    if pid > 0:
        try:
            pid_file = open('/var/run/rdpproxy.pid','w')
            pid_file.write(str(pid)+"\n")
            pid_file.close()
        finally:
            sys.exit()

    if pid == 0:
        main()

if __name__ == '__main__':
    daemonize()
