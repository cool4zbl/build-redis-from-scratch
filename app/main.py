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

            if data == b"*1\r\n$4\r\nPING\r\n":
                response = b"+PONG\r\n"
                writer.write(response)
                await writer.drain()
    except asyncio.CancelledError:
        pass
    finally:
        writer.close()
        await writer.wait_closed()
        print(f"Connection from {addr} closed. \n")

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
