import sys
from lsmtree import LSMTree
import socket
import threading


class Node:
    def __init__(
        self,
        storage_location: str = "storage/",
        host: str = "127.0.0.1",
        port: int = 65432,
    ):
        self.storage = LSMTree(storage_location)
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.host = host
        self.port = port

    def start_server(self):
        print(f"Starting server on {self.host}:{self.port}")

        try:
            self.server_socket.bind((self.host, self.port))
        except socket.error as e:
            print(str(e))
            sys.exit()

        self.server_socket.listen(5)
        print("Waiting for a connection.")

        while True:
            sock, address = self.server_socket.accept()
            print(f"Connected to: {address[0]}:{address[1]}")
            new_connection_thread = ConnectionThread(sock, self)
            new_connection_thread.start()

    def handle_message(self, client_socket: socket.socket, message: str):
        print(f"Received message: {message}")
        if message.startswith("get"):
            key = message.split(" ")[1]
            value = self.storage.get(key)
            if value is None:
                value = "None"
            client_socket.sendall(value.encode("utf-8"))
        elif message.startswith("put"):
            key, value = message.split(" ")[1:]
            self.storage.put(key, value)
            client_socket.sendall("OK".encode("utf-8"))
        elif message.startswith("delete"):
            key = message.split(" ")[1]
            self.storage.delete(key)
            client_socket.sendall("OK".encode("utf-8"))
        elif message.startswith("exit"):
            client_socket.sendall("OK".encode("utf-8"))
        else:
            client_socket.sendall("OK".encode("utf-8"))


class ConnectionThread(threading.Thread):
    def __init__(self, sock: socket.socket, node: Node):
        threading.Thread.__init__(self)
        self.node = node
        self.socket = sock
        self.socket.settimeout(3600)

    def run(self):
        while True:
            try:
                message = self.socket.recv(1024)
            except socket.timeout:
                print("Connection timed out")
                break

            message = message.decode("utf-8")
            if message == "exit":
                print("Closing connection")
                break

            self.node.handle_message(self.socket, message)

        self.socket.close()


if __name__ == "__main__":
    node = Node()
    node.start_server()
