import socket

if __name__ == "__main__":
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(("127.0.0.1", 65432))

        message = "put a 1"
        client_socket.send(message.encode())

        print("Sent: " + message)
        print("Received: " + client_socket.recv(1024).decode())

        message = "get a"
        client_socket.send(message.encode())

        print("Sent: " + message)
        print("Received: " + client_socket.recv(1024).decode())

        message = "put a 2"
        client_socket.send(message.encode())

        print("Sent: " + message)
        print("Received: " + client_socket.recv(1024).decode())

        message = "get a"
        client_socket.send(message.encode())

        print("Sent: " + message)
        print("Received: " + client_socket.recv(1024).decode())

        message = "delete a"
        client_socket.send(message.encode())

        print("Sent: " + message)
        print("Received: " + client_socket.recv(1024).decode())

        message = "get a"
        client_socket.send(message.encode())

        print("Sent: " + message)
        print("Received: " + client_socket.recv(1024).decode())

        # Send a message to tell server to end the connection
        end_message = "exit"
        client_socket.send(end_message.encode())

    except Exception as e:
        print("There was an error:", e)
    finally:
        # Close the connection
        client_socket.close()
