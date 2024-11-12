import asyncio

class Cache:
    def __init__(self):
        self.cache = {}

    def set(self, key, value):
        self.cache[key] = value

    async def set_with_ttl(self, key, value, ttl):
        self.cache[key] = value
        await asyncio.create_task(self.expire(key, ttl))

    async def expire(self, key, ttl):
        await asyncio.sleep(ttl / 1000)
        self.cache.pop(key, None)

    def get(self, key):
        return self.cache.get(key)

    def delete(self, key):
        return self.cache.pop(key, None)

    def flush(self):
        self.cache.clear()

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
        print(f"Connection from {addr} cancelled. \n")
    finally:
        writer.close()
        await writer.wait_closed()
        print(f"Connection from {addr} closed. \n")

def parse_request(data):
    parts = data.split(b"\r\n")

    print(f"Received request data, {parts}")

    data_type_switcher = {
        b"*": "array",
        b"$": "bulk_string",
        b"+": "simple_string",
        b"-": "error",
        b":": "integer"
    }
    command_switcher = {
        "PING": handle_ping,
        "ECHO": handle_echo,
        "SET": handle_set,
        "GET": handle_get
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

def handle_ping(parts):
    return b"+PONG\r\n"

def handle_echo(parts):
    print(f"Received ECHO string, {parts[4]}")
    # e.g. *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
    return f"${len(parts[4].decode())}\r\n{parts[4].decode()}\r\n".encode()

cache = Cache()
def handle_set(parts):
    # e.g. *3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n$2\r\nPX\r\n:100\r\n
    key, value = parts[4].decode(), parts[6].decode()
    print(f"Received SET command, key={key} and value={value}", parts[8].decode(), parts[9], parts[10])
    if parts[8].decode() == "PX":
        ttl = int(parts[9][1:].decode())
        print(f"Received TTL, {ttl}")
        asyncio.create_task(cache.set_with_ttl(key, value, ttl))
    else:
        cache.set(key, value)
    return b"+OK\r\n"

def handle_get(parts):
    key = parts[4].decode()
    print(f"Received GET command, key={key}")
    value = cache.get(key)
    return f"${len(value)}\r\n{value}\r\n".encode() if value else b"$-1\r\n"


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
