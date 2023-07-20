import socket
import sys
import threading

from .lsmtree import LSMTree


class Node:
    """
    A node in the distributed key-value store.
    """

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
        """
        Starts the server and listens for multiple connections.
        When a connection is established, a new thread is created to handle it.
        """
        print(f"Starting server on {self.host}:{self.port}")

        try:
            self.server_socket.bind((self.host, self.port))
        except socket.error as e:
            print(str(e))
            sys.exit()

        self.server_socket.listen(5)
        print("Waiting for a connection.")

        try:
            while True:
                sock, address = self.server_socket.accept()
                print(f"Connected to: {address[0]}:{address[1]}")
                new_connection_thread = ConnectionThread(sock, self)
                new_connection_thread.start()
        except KeyboardInterrupt:
            print("Closing server")
            self.server_socket.close()
            sys.exit()

    def handle_message(self, client_socket: socket.socket, message: str):
        """
        Handles a message received from a client on a connection thread.
        The message is parsed and the appropriate action is taken.
        """
        print(f"Received message: {message} from {client_socket.getpeername()}")

        if message.startswith("get"):
            key = message.split(" ")[1]
            value = self.storage.get(key)
            if value is None:
                value = f"key '{key}' not found"
            if isinstance(value, str):
                client_socket.sendall(str(value).encode("utf-8"))

        elif message.startswith("put"):
            key, value = message.split(" ")[1:]
            self.storage.put(key, value)
            client_socket.sendall("OK".encode("utf-8"))

        elif message.startswith("delete"):
            key = message.split(" ")[1]
            self.storage.delete(key)
            client_socket.sendall("OK".encode("utf-8"))

        else:
            client_socket.sendall(
                'Unrecognized command. Type "exit" to exit'.encode("utf-8")
            )


class ConnectionThread(threading.Thread):
    """
    A thread that handles a connection to a client.
    """

    def __init__(self, sock: socket.socket, node: Node):
        threading.Thread.__init__(self)
        self.node = node
        self.socket = sock
        self.socket.settimeout(3600)

    def run(self):
        """
        The main loop of the thread.
        Waits for messages from the client and hands them to the server node to process them.
        """
        while True:
            try:
                message = self.socket.recv(1024)
            except socket.timeout:
                print("Connection timed out")
                break

            decoded_message = message.decode("utf-8")
            if message == "exit":
                print("Closing connection")
                break

            self.node.handle_message(self.socket, decoded_message)

        self.socket.close()
