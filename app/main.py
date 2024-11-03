import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn, addr = server_socket.accept() # wait for client

    byte_received = 0
    MSG_SIZE = min(4, len(b"PING\nPING"))
    while byte_received < MSG_SIZE:
        conn.recv(MSG_SIZE - byte_received)
        conn.send(b"+PONG\r\n")


if __name__ == "__main__":
    main()
