import socket
import threading
import time

# In-memory key-value store shared by all client connections.
# Maps key -> (value_bytes, expires_at_ms) where expires_at_ms is a
# time.monotonic() deadline in ms, or None if the key never expires.
store = {}
store_lock = threading.Lock()

# In-memory lists shared by all client connections.
# Maps key -> list of value bytes (order matters: index 0 is the head).
lists = {}
lists_lock = threading.Lock()


def now_ms():
    return time.monotonic() * 1000


def get_live_value(key):
    """Return the value for key if it exists and hasn't expired, else None."""
    with store_lock:
        entry = store.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if expires_at is not None and now_ms() >= expires_at:
            del store[key]  # lazily evict expired key
            return None
        return value


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


def encode_resp_array(values) -> bytes:
    """Encode a sequence of byte strings as a RESP array."""
    return b"*" + str(len(values)).encode() + b"\r\n" + b"".join(
        encode_bulk_string(v) for v in values
    )


def lrange_bounds(start: int, stop: int, length: int):
    """Normalize Redis LRANGE indexes (possibly negative) to non-negative,
    inclusive bounds. Negative out-of-range indexes are clamped to 0."""
    if start < 0:
        start = max(length + start, 0)
    if stop < 0:
        stop = max(length + stop, 0)
    return start, stop


def execute_command(args):
    """Execute a parsed command (list of byte-string arguments)."""
    command = args[0].decode("utf-8", "replace").lower() if args else ""
    if command == "ping":
        return b"+PONG\r\n"
    if command == "echo":
        value = args[1] if len(args) > 1 else b""
        return encode_bulk_string(value)
    if command == "set" and len(args) >= 3:
        expires_at = None
        i = 3
        while i < len(args):
            option = args[i].decode("utf-8", "replace").lower()
            if option in ("px", "ex") and i + 1 < len(args):
                try:
                    duration_ms = int(args[i + 1])
                    if option == "ex":
                        duration_ms *= 1000
                except ValueError:
                    return b"-ERR value is not an integer or out of range\r\n"
                if duration_ms > 0:
                    expires_at = now_ms() + duration_ms
                i += 2
            else:
                break  # unknown/unsupported option: ignore for now
        with store_lock:
            store[args[1]] = (args[2], expires_at)
        return b"+OK\r\n"
    if command == "get" and len(args) >= 2:
        value = get_live_value(args[1])
        return encode_bulk_string(value) if value is not None else b"$-1\r\n"
    if command == "rpush" and len(args) >= 3:
        key, values = args[1], args[2:]
        with lists_lock:
            lst = lists.setdefault(key, [])
            lst.extend(values)
            length = len(lst)
        return b":" + str(length).encode() + b"\r\n"
    if command == "lpush" and len(args) >= 3:
        key, values = args[1], args[2:]
        with lists_lock:
            lst = lists.setdefault(key, [])
            # Push elements one by one so the last listed value ends up
            # at the head: LPUSH k a b c -> [c, b, a].
            for value in values:
                lst.insert(0, value)
            length = len(lst)
        return b":" + str(length).encode() + b"\r\n"
    if command == "llen" and len(args) >= 2:
        with lists_lock:
            length = len(lists.get(args[1], []))
        return b":" + str(length).encode() + b"\r\n"
    if command == "lrange" and len(args) >= 4:
        key = args[1]
        try:
            start, stop = int(args[2]), int(args[3])
        except ValueError:
            return b"-ERR value is not an integer or out of range\r\n"
        # Normalize negative indexes, then slice. Covers all required rules:
        # start > stop or start >= len -> empty; stop >= len -> clamped.
        with lists_lock:
            lst = lists.get(key, [])
            start_n, stop_n = lrange_bounds(start, stop, len(lst))
            elements = lst[start_n:stop_n + 1]
        return encode_resp_array(elements)
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
