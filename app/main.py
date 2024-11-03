import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn, addr = server_socket.accept() # wait for client

    byte_received = 0
    MSG_SIZE = len(b"PING\nPING")
    while byte_received < MSG_SIZE:
        chunk = conn.recv(min(MSG_SIZE - byte_received, 4))
        conn.send(b"+PONG\r\n")
        byte_received += len(chunk)


if __name__ == "__main__":
    main()
