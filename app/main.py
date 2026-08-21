import socket
import threading
import time
from collections import deque

# In-memory key-value store shared by all client connections.
# Maps key -> (value_bytes, expires_at_ms) where expires_at_ms is a
# time.monotonic() deadline in ms, or None if the key never expires.
store = {}
store_lock = threading.Lock()

# In-memory lists shared by all client connections.
# Maps key -> list of value bytes (order matters: index 0 is the head).
lists = {}
lists_lock = threading.Lock()

# Blocked BLPOP clients, FIFO per list key (longest-waiting first).
blpop_waiters = {}  # key -> deque of _BlpopWaiter

# In-memory streams shared by all client connections.
# Maps key -> list of entries in chronological order,
# where each entry is (id_bytes, [(field_bytes, value_bytes), ...]).
streams = {}
streams_lock = threading.Lock()


class _BlpopWaiter:
    def __init__(self):
        self.event = threading.Event()
        self.served = False
        self.key = None
        self.value = None


class _XreadWaiter:
    def __init__(self):
        self.event = threading.Event()


# Blocked XREAD clients; every XADD wakes them all and each re-scans.
xread_waiters = []


def serve_blpop_waiters(key):
    """Hand list elements directly to blocked BLPOP clients (FIFO).
    Must be called with lists_lock held."""
    queue = blpop_waiters.get(key)
    lst = lists.get(key)
    while queue and lst:
        waiter = queue.popleft()
        waiter.key = key
        waiter.value = lst.pop(0)
        waiter.served = True
        waiter.event.set()


def notify_xread_waiters():
    """Wake all blocked XREAD clients so they re-scan the streams.
    Must be called with streams_lock held."""
    for waiter in xread_waiters:
        waiter.event.set()


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


def parse_stream_id(entry_id: bytes):
    """Parse a stream ID '<millisecondsTime>-<sequenceNumber>' into an
    (ms, seq) integer tuple, or None if malformed."""
    parts = entry_id.split(b"-")
    if len(parts) != 2:
        return None
    try:
        ms, seq = int(parts[0]), int(parts[1])
    except ValueError:
        return None
    if ms < 0 or seq < 0:
        return None
    return ms, seq


def parse_xrange_bound(bound: bytes, is_start: bool):
    """Parse an XRANGE range bound into an (ms, seq) tuple.
    '-' is the minimum ID, '+' the maximum. Incomplete IDs default the
    sequence to 0 for start bounds and unbounded for end bounds."""
    if bound == b"-":
        return (0, 0)
    if bound == b"+":
        return (float("inf"), float("inf"))
    parts = bound.split(b"-")
    try:
        ms = int(parts[0])
        if len(parts) == 1:
            return (ms, 0 if is_start else float("inf"))
        if len(parts) == 2:
            return (ms, int(parts[1]))
    except ValueError:
        pass
    return None


def encode_stream_entry(entry) -> bytes:
    """Encode one stream entry as a RESP 2-element array:
    [id, [field1, value1, field2, value2, ...]]."""
    entry_id, fields = entry
    flat = b"".join(
        encode_bulk_string(item) for pair in fields for item in pair
    )
    return (
        b"*2\r\n"
        + encode_bulk_string(entry_id)
        + b"*" + str(len(fields) * 2).encode() + b"\r\n"
        + flat
    )


def execute_command(args, tx=None):
    """Execute a parsed command (list of byte-string arguments).

    `tx` is the calling connection's transaction state
    ({"active": bool, "queue": [args, ...]}), or None for callers
    without a connection (e.g. internal use/tests).
    """
    command = args[0].decode("utf-8", "replace").lower() if args else ""
    if command == "ping":
        return b"+PONG\r\n"
    if command == "multi":
        # Start a transaction for this connection.
        if tx is not None:
            tx["active"] = True
            tx["queue"] = []
        return b"+OK\r\n"
    if command == "exec":
        if tx is None or not tx.get("active"):
            return b"-ERR EXEC without MULTI\r\n"
        tx["active"] = False
        # Execute every queued command; EXEC replies with an array of
        # their responses (empty array when nothing was queued).
        responses = [execute_command(cmd, tx) for cmd in tx.get("queue", [])]
        return b"*" + str(len(responses)).encode() + b"\r\n" + b"".join(responses)
    if command == "discard":
        # Must be handled before queueing so it works mid-transaction.
        if tx is None or not tx.get("active"):
            return b"-ERR DISCARD without MULTI\r\n"
        tx["active"] = False
        tx["queue"] = []
        return b"+OK\r\n"
    if command == "watch" and len(args) >= 2:
        # Optimistic locking: WATCH is only allowed outside a transaction.
        if tx is not None and tx.get("active"):
            return b"-ERR WATCH inside MULTI is not allowed\r\n"
        if tx is not None:
            tx.setdefault("watched", set()).update(args[1:])
        return b"+OK\r\n"
    # A transaction is active: queue every other command instead of
    # executing it, so the database stays untouched until EXEC.
    if tx is not None and tx.get("active"):
        tx["queue"].append(args)
        return b"+QUEUED\r\n"
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
    if command == "incr" and len(args) >= 2:
        key = args[1]
        with store_lock:
            entry = store.get(key)
            if entry is not None:
                value, expires_at = entry
                if expires_at is not None and now_ms() >= expires_at:
                    del store[key]  # lazily evict expired key
                    entry = None
            if entry is None:
                # Missing key: INCR initializes it to 1.
                store[key] = (b"1", None)
                return b":1\r\n"
            value, expires_at = entry
            try:
                current = int(value)
            except ValueError:
                return b"-ERR value is not an integer or out of range\r\n"
            new_value = current + 1
            # Preserve any existing expiry, like real Redis.
            store[key] = (str(new_value).encode(), expires_at)
            return b":" + str(new_value).encode() + b"\r\n"
    if command == "rpush" and len(args) >= 3:
        key, values = args[1], args[2:]
        with lists_lock:
            lst = lists.setdefault(key, [])
            lst.extend(values)
            length = len(lst)
            serve_blpop_waiters(key)
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
            serve_blpop_waiters(key)
        return b":" + str(length).encode() + b"\r\n"
    if command == "llen" and len(args) >= 2:
        with lists_lock:
            length = len(lists.get(args[1], []))
        return b":" + str(length).encode() + b"\r\n"
    if command == "lpop" and len(args) >= 2:
        count = None
        if len(args) >= 3:
            try:
                count = int(args[2])
            except ValueError:
                return b"-ERR value is not an integer or out of range\r\n"
            if count <= 0:
                return b"-ERR value is out of range, must be positive\r\n"
        with lists_lock:
            lst = lists.get(args[1])
            if not lst:
                # With a count arg, missing/empty list -> empty array;
                # without -> null bulk string.
                return encode_resp_array([]) if count is not None else b"$-1\r\n"
            n = 1 if count is None else min(count, len(lst))
            popped = lst[:n]
            del lst[:n]
            if not lst:  # drop empty lists, like real Redis
                del lists[args[1]]
        if count is None:
            return encode_bulk_string(popped[0])
        return encode_resp_array(popped)
    if command == "blpop" and len(args) >= 3:
        keys = args[1:-1]
        try:
            timeout = float(args[-1])
        except ValueError:
            return b"-ERR timeout is not a float or out of range\r\n"
        deadline = None if timeout <= 0 else time.monotonic() + timeout
        with lists_lock:
            # Serve immediately if any requested list has elements.
            for key in keys:
                lst = lists.get(key)
                if lst:
                    value = lst.pop(0)
                    if not lst:
                        del lists[key]
                    return encode_resp_array([key, value])
            # Nothing available: register as a waiter on every key.
            waiter = _BlpopWaiter()
            for key in keys:
                blpop_waiters.setdefault(key, deque()).append(waiter)
        # Block until served or timed out (timeout 0 waits indefinitely).
        while True:
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                break
            if waiter.event.wait(remaining) and waiter.served:
                return encode_resp_array([waiter.key, waiter.value])
            if not waiter.event.is_set():
                break  # timed out
        with lists_lock:
            for key in keys:
                queue = blpop_waiters.get(key)
                if queue:
                    try:
                        queue.remove(waiter)
                    except ValueError:
                        pass
        return b"*-1\r\n"
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
    if command == "type" and len(args) >= 2:
        key = args[1]
        with lists_lock:
            if key in lists:
                return b"+list\r\n"
        with streams_lock:
            if key in streams:
                return b"+stream\r\n"
        if get_live_value(key) is not None:
            return b"+string\r\n"
        return b"+none\r\n"
    if command == "xadd" and len(args) >= 5:
        key, id_arg = args[1], args[2]
        pairs = args[3:]
        if len(pairs) % 2 != 0:
            return b"-ERR wrong number of arguments for 'xadd' command\r\n"
        fields = list(zip(pairs[0::2], pairs[1::2]))
        invalid_err = b"-ERR Invalid stream ID specified as stream command argument\r\n"
        with streams_lock:
            entries = streams.setdefault(key, [])
            last_id = parse_stream_id(entries[-1][0]) if entries else None

            if id_arg == b"*":
                # Fully auto-generated ID: current Unix time in ms,
                # sequence continues from the last entry in the same ms.
                ms = int(time.time() * 1000)
                if last_id is not None and last_id[0] == ms:
                    seq = last_id[1] + 1
                else:
                    seq = 0
                new_id = (ms, seq)
                entry_id = f"{ms}-{seq}".encode()
            elif id_arg.endswith(b"-*"):
                # Auto-generate only the sequence number (<ms>-*).
                try:
                    ms = int(id_arg[:-2])
                except ValueError:
                    return invalid_err
                if ms < 0:
                    return invalid_err
                if last_id is not None and last_id[0] == ms:
                    seq = last_id[1] + 1  # continue the same millisecond
                elif ms == 0:
                    seq = 1  # 0-0 is never valid
                else:
                    seq = 0
                new_id = (ms, seq)
                entry_id = f"{ms}-{seq}".encode()
            else:
                # Explicit ID (<ms>-<seq>).
                new_id = parse_stream_id(id_arg)
                if new_id is None:
                    return invalid_err
                entry_id = id_arg

            if new_id <= (0, 0):
                return b"-ERR The ID specified in XADD must be greater than 0-0\r\n"
            if last_id is not None and new_id <= last_id:
                return (
                    b"-ERR The ID specified in XADD is equal or smaller "
                    b"than the target stream top item\r\n"
                )
            entries.append((entry_id, fields))
            notify_xread_waiters()  # wake blocked XREAD clients
        return encode_bulk_string(entry_id)
    if command == "xrange" and len(args) >= 4:
        key, start_arg, end_arg = args[1], args[2], args[3]
        start_id = parse_xrange_bound(start_arg, is_start=True)
        end_id = parse_xrange_bound(end_arg, is_start=False)
        if start_id is None or end_id is None:
            return b"-ERR Invalid stream ID specified as stream command argument\r\n"
        # Optional COUNT <n> argument.
        count = None
        i = 4
        while i < len(args):
            opt = args[i].decode("utf-8", "replace").lower()
            if opt == "count" and i + 1 < len(args):
                try:
                    count = int(args[i + 1])
                except ValueError:
                    return b"-ERR value is not an integer or out of range\r\n"
                i += 2
            else:
                break
        matched = []
        with streams_lock:
            for entry in streams.get(key, []):
                eid = parse_stream_id(entry[0])
                if eid is None:
                    continue
                if start_id <= eid <= end_id:
                    matched.append(entry)
                    if count is not None and len(matched) >= count:
                        break
        body = b"".join(encode_stream_entry(e) for e in matched)
        return b"*" + str(len(matched)).encode() + b"\r\n" + body
    if command == "xread":
        # Parse optional arguments before the STREAMS keyword.
        count = None
        block_ms = None
        i = 1
        while i < len(args):
            opt = args[i].decode("utf-8", "replace").lower()
            if opt == "count" and i + 2 < len(args):
                try:
                    count = int(args[i + 1])
                except ValueError:
                    return b"-ERR value is not an integer or out of range\r\n"
                i += 2
            elif opt == "block" and i + 2 < len(args):
                try:
                    block_ms = int(args[i + 1])
                except ValueError:
                    return b"-ERR value is not an integer or out of range\r\n"
                i += 2
            elif opt == "streams":
                i += 1
                break
            else:
                return b"-ERR syntax error\r\n"
        else:
            return b"-ERR syntax error\r\n"
        tail = args[i:]
        if not tail or len(tail) % 2 != 0:
            return b"-ERR wrong number of arguments for 'xread' command\r\n"
        half = len(tail) // 2
        keys, ids = tail[:half], tail[half:]

        # Resolve IDs once, up front ('$' = last entry id at command time).
        resolved_ids = []

        def scan_locked():
            """Collect matching entries per key. Assumes streams_lock held."""
            results = []
            for key, start_id in zip(keys, resolved_ids):
                matched = []
                for entry in streams.get(key, []):
                    eid = parse_stream_id(entry[0])
                    if eid is not None and eid > start_id:  # exclusive
                        matched.append(entry)
                        if count is not None and len(matched) >= count:
                            break
                if matched:
                    body = b"".join(encode_stream_entry(e) for e in matched)
                    results.append(
                        b"*2\r\n"
                        + encode_bulk_string(key)
                        + b"*" + str(len(matched)).encode() + b"\r\n"
                        + body
                    )
            return results

        def encode_results(results):
            if not results:
                return b"*-1\r\n"  # null array when no stream had entries
            return b"*" + str(len(results)).encode() + b"\r\n" + b"".join(results)

        with streams_lock:
            for idx, id_arg in enumerate(ids):
                if id_arg == b"$":
                    # Only entries added after this command can match.
                    entries = streams.get(keys[idx], [])
                    resolved_ids.append(
                        parse_stream_id(entries[-1][0]) if entries else (0, 0)
                    )
                else:
                    start_id = parse_stream_id(id_arg)
                    if start_id is None:
                        return (
                            b"-ERR Invalid stream ID specified as "
                            b"stream command argument\r\n"
                        )
                    resolved_ids.append(start_id)
            results = scan_locked()

        if results or block_ms is None:
            return encode_results(results)

        # Blocking mode: wait for an XADD, re-scanning after every wake-up.
        # block_ms == 0 blocks indefinitely.
        deadline = None if block_ms <= 0 else time.monotonic() + block_ms / 1000.0
        while True:
            waiter = _XreadWaiter()
            with streams_lock:
                # Register before scanning so an XADD racing with us cannot
                # be missed: it either lands before the scan or wakes us.
                xread_waiters.append(waiter)
                results = scan_locked()
                if results:
                    xread_waiters.remove(waiter)
            if results:
                break
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                with streams_lock:
                    try:
                        xread_waiters.remove(waiter)
                    except ValueError:
                        pass
                return b"*-1\r\n"
            waiter.event.wait(remaining)
        return encode_results(results)
    return b"-ERR unknown command\r\n"


def handle_connection(conn):
    # Per-connection transaction state (MULTI/EXEC/WATCH).
    tx = {"active": False, "queue": [], "watched": set()}
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
                    conn.sendall(execute_command(args, tx))
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
