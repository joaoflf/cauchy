import socket

if __name__ == "__main__":
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_host = "127.0.0.1"
        server_port = 65432
        client_socket.connect((server_host, server_port))
        print("Connected to server ", server_host, ":", server_port)

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

        client_socket.sendall("exit".encode())
        client_socket.close()

    except Exception as e:
        print("There was an error:", e)
    finally:
        # Close the connection
        client_socket.close()
