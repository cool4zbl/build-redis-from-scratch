import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn, addr = server_socket.accept() # wait for client

    while True:
        data = conn.recv(1024)
        if not data:
            break
        if data.lower().find(b"ping") != -1:
            conn.send(b"+PONG\r\n")


if __name__ == "__main__":
    main()
