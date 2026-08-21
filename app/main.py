import socket
import threading


def parse_resp_array(buffer):
    """Try to parse one RESP array from the front of `buffer`.

    Returns (list_of_bytes_args, remaining_buffer), or (None, buffer)
    if the buffer doesn't yet contain a complete command.
    """
    if not buffer.startswith(b"*"):
        return None, buffer
    line_end = buffer.find(b"\r\n")
    if line_end == -1:
        return None, buffer
    try:
        num_elements = int(buffer[1:line_end])
    except ValueError:
        return None, buffer

    pos = line_end + 2
    args = []
    for _ in range(num_elements):
        if buffer[pos:pos + 1] != b"$":
            return None, buffer
        bulk_line_end = buffer.find(b"\r\n", pos)
        if bulk_line_end == -1:
            return None, buffer
        try:
            length = int(buffer[pos + 1:bulk_line_end])
        except ValueError:
            return None, buffer
        start = bulk_line_end + 2
        end = start + length
        if len(buffer) < end + 2:  # payload + trailing \r\n
            return None, buffer
        args.append(buffer[start:end])
        pos = end + 2
    return args, buffer[pos:]


def encode_bulk_string(value: bytes) -> bytes:
    """Encode bytes as a RESP bulk string: $<len>\r\n<data>\r\n."""
    return b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"


def execute_command(args):
    """Execute a parsed command (list of byte-string arguments)."""
    command = args[0].decode("utf-8", "replace").lower() if args else ""
    if command == "ping":
        return b"+PONG\r\n"
    if command == "echo":
        value = args[1] if len(args) > 1 else b""
        return encode_bulk_string(value)
    return b"-ERR unknown command\r\n"


def handle_connection(conn):
    with conn:
        buffer = b""
        try:
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                buffer += data
                while True:
                    args, buffer = parse_resp_array(buffer)
                    if args is None:
                        break
                    conn.sendall(execute_command(args))
        except (ConnectionResetError, BrokenPipeError):
            pass  # client disconnected abruptly


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        conn, _addr = server_socket.accept()  # wait for client
        threading.Thread(target=handle_connection, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()
