import socket  # noqa: F401
import concurrent.futures  # noqa: F401

def handle_client(conn, addr):
    print(f"Connection from {addr} opened.")
    while True:
        data = conn.recv(1024)
        if not data:
            break
        if data == b"*1\r\n$4\r\nPING\r\n":
            conn.send(b"+PONG\r\n")
    conn.close()
    print(f"Connection from {addr} closed.")


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        while True:
            conn, addr = server_socket.accept() # wait for client
            executor.submit(handle_client, conn, addr)


if __name__ == "__main__":
    main()
