import fire

from .client import Client
from .server import Node


def server(
    host: str = "127.0.0.1", port: int = 65432, storage_location: str = "storage/"
):
    """
    Starts a server on the specified host and port.
    """
    node = Node(storage_location, host, port)
    node.start_server()


def connect(host: str = "127.0.0.1", port: int = 65432):
    """
    Connects to a server on the specified host and port.
    """
    client = Client(host, port)
    client.connect()


def main():
    fire.Fire({"server": server, "connect": connect})
