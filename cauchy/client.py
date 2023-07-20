import socket


class Client:
    """
    A client that connects to a server node and queries it for data.
    """

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def connect(self):
        """
        Opens a socket connection and provides a REPL for the user to query the server.
        The exit command closes the connection.
        """
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client_socket.connect((self.host, self.port))
            print("Connected to server ", self.host, ":", self.port)

            while True:
                # Get the user's input
                message = input("> ")

                # If the user types 'exit', break out of the loop
                if message.lower() == "exit":
                    break

                # Send the message to the server
                client_socket.sendall(message.encode())

                # Get the server's response
                print(client_socket.recv(1024).decode())

            client_socket.close()
            print("Connection closed")

        except Exception as e:
            print("There was an error:", e)
        finally:
            # Close the connection
            client_socket.close()
