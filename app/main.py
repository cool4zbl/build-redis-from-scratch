import asyncio

async def handle_client(reader, writer):
    addr = writer.get_extra_info("peername")
    print(f"Connection from {addr}. \n")

    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break
            print(f"Received {data.decode()!r} from {addr}. \n")

            response = parse_request(data)
            writer.write(response)
            await writer.drain()
    except asyncio.CancelledError:
        pass
    finally:
        writer.close()
        await writer.wait_closed()
        print(f"Connection from {addr} closed. \n")

def parse_request(data):
    parts = data.split(b"\r\n")
    print(f"Received parts, {parts}")

    data_type_switcher = {
        b"*": "array",
        b"$": "bulk_string",
        b"+": "simple_string",
        b"-": "error",
        b":": "integer"
    }
    command_switcher = {
        "PING": handle_ping,
        "ECHO": handle_echo
    }

    type = data_type_switcher.get(parts[0][0:1], "error")
    if type == "error":
        print("Invalid request, no valid data type found.")
        return None
    if type == "array":
        print("Received an array.")
        # num_args = int(parts[0][1:])
        command = parts[2].decode().upper()
        res = command_switcher.get(command, "error")(parts)
        return res
    else:
        return None

def handle_ping():
    return b"+PONG\r\n"

def handle_echo(parts):
    print(f"Received ECHO string, {parts[4]}")
    # e.g. *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
    return f"${len(parts[4].decode())}\r\n{parts[4].decode()}\r\n".encode()


async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server = await asyncio.start_server(handle_client, host="localhost", port=6379, reuse_port=True)
    addr = server.sockets[0].getsockname()
    print(f"Server started at {addr}\n")

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer stopped.")
