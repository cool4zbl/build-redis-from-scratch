import asyncio
import argparse

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


default_dir, default_dbfilename = "/tmp/redis-files", "dump.rdb"
configs = {
    "dir": default_dir,
    "dbfilename": default_dbfilename,
}

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
        "GET": handle_get,
        "CONFIG": handle_config
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
    # e.g. "*5\r\n$3\r\nSET\r\n$6\r\nbanana\r\n$9\r\nblueberry\r\n$2\r\npx\r\n$3\r\n100\r\n"
    key, value = parts[4].decode(), parts[6].decode()
    print(f"Received SET command, key={key} and value={value}")

    if len(parts) >= 10 and parts[8].decode().upper() == "PX":
        ttl = int(parts[10])
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

def handle_config(parts):
    command, key = parts[4].decode().upper(), parts[6].decode()
    if command == 'GET':
        # *2\r\n$3\r\ndir\r\n$16\r\n/tmp/redis-files\r\n
        return f"*2\r\n${len(key)}\r\n{key}\r\n${len(configs[key])}\r\n{configs[key]}\r\n".encode()
        # match key:
        #     case 'dir':
        #         return f"${len(configs["dir"])}\r\n{configs["dir"]}\r\n".encode()
        #     case 'dbfilename':
        #         return f"${len(configs["dbfilename"])}\r\n{configs["dbfilename"]}\r\n".encode()

async def run_server():
    server = await asyncio.start_server(handle_client, host="localhost", port=6379, reuse_port=True)
    addr = server.sockets[0].getsockname()
    print(f"Server started at {addr}\n")

    async with server:
        await server.serve_forever()

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    parser = argparse.ArgumentParser(description="Parse Redis cli commands")
    parser.add_argument("--dir", type=str, default=default_dir, help="Redis RDB dir")
    parser.add_argument("--dbfilename", type=str, default=default_dbfilename, help="Redis RDB dir")

    args = parser.parse_args()
    configs["dir"] = args.dir
    configs["dbfilename"] = args.dbfilename

    try:
        asyncio.run(run_server())
    except KeyboardInterrupt:
        print("\nServer stopped.")

if __name__ == "__main__":
    main()
