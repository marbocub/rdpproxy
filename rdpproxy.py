#!/usr/bin/env python
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

class DummyTCP(threading.Thread):
    def __init__(self, client_sock, client_address, client_port):
        threading.Thread.__init__(self, daemon=True, name="dummy")
        self.client_sock = client_sock
        self.client_address = client_address
        self.client_port = client_port
        self.bufsize = 4096

    def run(self):
        readfds = set([self.client_sock])
        while True:
            rready, wready, xready = select.select(readfds, [], [], 10)
            for sock in rready:
                try:
                    msg = sock.recv(self.bufsize)
                    if len(msg) == 0:
                        self.close_socket()
                        return
                except Exception as e:
                    self.close_socket()
                    return

    def get_client_address(self):
        return self.client_address

    def close_socket(self):
        try:
            self.client_sock.close()
        except Exception as e:
            pass

class ProxyTCP(threading.Thread):
    def __init__(self, client_sock, client_address, client_port, server_address, server_port):
        threading.Thread.__init__(self, daemon=True, name="worker")
        self.client_sock = client_sock
        self.client_address = client_address
        self.client_port = client_port
        self.server_address = server_address
        self.server_port = server_port
        self.bufsize = 4096
        self.pool = Pool()
        try:
            self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_sock.connect((server_address, server_port))
        except Exception as e:
            raise

    def run(self):
        readfds = set([self.client_sock, self.server_sock])
        while True:
            rready, wready, xready = select.select(readfds, [], [], 10)
            for sock in rready:
                if sock is self.server_sock:
                    try:
                        msg = sock.recv(self.bufsize)
                        if len(msg) == 0:
                            self.close_socket()
                            return
                        self.client_sock.send(msg)
                    except Exception as e:
                        self.close_socket()
                        return
                elif sock is self.client_sock:
                    try:
                        msg = sock.recv(self.bufsize)
                        if len(msg) == 0:
                            self.close_socket()
                            return
                        self.server_sock.send(msg)
                    except Exception as e:
                        self.close_socket()
                        return

    def get_server_address(self):
        return self.server_address

    def get_client_address(self):
        return self.client_address

    def close_socket(self):
        self.client_address = ''
        try:
            self.client_sock.close()
        except Exception as e:
            pass
        try:
            self.server_sock.close()
        except Exception as e:
            pass
        self.pool.register_client(self.server_address, '')

class Pool():
    database = 'db.sqlite3'

    def init_database(self):
        db = sqlite3.connect(self.database)
        db.execute('''create table if not exists pool (server text primary key, weight int, client text, wait_start datetime)''')
        db.close()

    def get_server(self, client_address):
        db = sqlite3.connect(self.database)
        cur = db.cursor()

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
    pool = Pool()
    pool.init_database()

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
            client_sock, (client_address, client_port) = tcp.accept()
            server_address = pool.get_server(client_address)
            if server_address == None:
                th = DummyTCP(client_sock, client_address, client_port)
                th.start()
            else:
                th = ProxyTCP(client_sock, client_address, client_port, server_address, server_port)
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
