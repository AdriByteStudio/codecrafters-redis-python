import socket
import threading
import time
import os
import math
from collections import deque

# Empty RDB file (hex) - used for full resynchronization
EMPTY_RDB_HEX = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
EMPTY_RDB = bytes.fromhex(EMPTY_RDB_HEX)

# In-memory key-value store shared by all client connections.
# Maps key -> (value_bytes, expires_at_ms) where expires_at_ms is a
# time.monotonic() deadline in ms, or None if the key never expires.
store = {}
store_lock = threading.Lock()

# Server role: "master" by default, "slave" when --replicaof is set.
server_role = "master"

# RDB persistence config (set via CLI args).
config_dir = os.getcwd()
config_dbfilename = "dump.rdb"

# AOF persistence config defaults.
config_appendonly = "no"
config_appenddirname = "appendonlydir"
config_appendfilename = "appendonly.aof"
config_appendfsync = "everysec"

# Active AOF file path (read from manifest on startup, None if not enabled).
aof_file_path = None

# Connected replicas (list of socket objects for propagation).
replica_connections = []
replica_connections_lock = threading.Lock()

# Master replication offset: total bytes of commands sent to replicas.
master_repl_offset = 0

# Last ACK offset received from each replica, keyed by socket id.
replica_ack_offsets = {}  # id(conn) -> last acknowledged offset
replica_ack_lock = threading.Lock()

# In-memory lists shared by all client connections.
# Maps key -> list of value bytes (order matters: index 0 is the head).
lists = {}
lists_lock = threading.Lock()

# Blocked BLPOP clients, FIFO per list key (longest-waiting first).
blpop_waiters = {}  # key -> deque of _BlpopWaiter

# Pub/Sub state: maps channel name (bytes) -> set of subscriber socket ids.
channels_subscribers = {}
channels_lock = threading.Lock()

# In-memory streams shared by all client connections.
# Maps key -> list of entries in chronological order,
# where each entry is (id_bytes, [(field_bytes, value_bytes), ...]).
streams = {}
streams_lock = threading.Lock()

# Sorted sets: maps key -> list of (score, member) tuples sorted by score.
sorted_sets = {}
sorted_sets_lock = threading.Lock()

# WATCH (optimistic locking) registrations:
# maps watched key -> set of _Watcher objects watching that key.
watched_keys = {}
watched_lock = threading.Lock()

# Geohash constants for encode/decode.
MIN_LAT = -85.05112878
MAX_LAT = 85.05112878
MIN_LON = -180.0
MAX_LON = 180.0
LAT_RANGE = MAX_LAT - MIN_LAT
LON_RANGE = MAX_LON - MIN_LON


def geo_decode(score):
    """Decode a geohash score back to (longitude, latitude)."""
    geo_code = int(score)
    # Extract lat bits (even positions) and lon bits (odd positions)
    x = geo_code
    y = geo_code >> 1

    def compact64_to_32(v):
        v &= 0x5555555555555555
        v = (v | (v >> 1)) & 0x3333333333333333
        v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F
        v = (v | (v >> 4)) & 0x00FF00FF00FF00FF
        v = (v | (v >> 8)) & 0x0000FFFF0000FFFF
        v = (v | (v >> 16)) & 0x00000000FFFFFFFF
        return v

    grid_lat = compact64_to_32(x)
    grid_lon = compact64_to_32(y)

    lat = MIN_LAT + LAT_RANGE * ((grid_lat + 0.5) / (1 << 26))
    lon = MIN_LON + LON_RANGE * ((grid_lon + 0.5) / (1 << 26))
    return lon, lat


def load_rdb_file(filepath):
    """Parse a Redis RDB file and populate the in-memory store.

    Handles the subset of RDB format needed for this challenge:
    - Header (REDIS0011)
    - Metadata subsections (FA ...)
    - Database subsections (FE ...) with hash table sizes (FB ...)
    - Key-value entries with optional expiry (FC/FD) and string values (type 0x00)
    - End-of-file marker (FF)

    String encoding supports:
    - Standard size-prefixed strings (sizes 0b00, 0b01, 0b10)
    - 8-bit integer  (0xC0) -> str(int)
    - 16-bit integer (0xC1) -> str(int)
    - 32-bit integer (0xC2) -> str(int)
    """
    try:
        with open(filepath, "rb") as f:
            data = f.read()
    except FileNotFoundError:
        return

    if len(data) < 9:
        return

    # Validate header: "REDIS0011"
    if data[:9] != b"REDIS0011":
        return

    pos = 9

    def read_length(data, pos):
        """Read a length-encoded value. Returns (length, new_pos)."""
        if pos >= len(data):
            return None, pos
        first = data[pos]
        type_bits = (first >> 6) & 0b11
        if type_bits == 0b00:
            return first & 0x3F, pos + 1
        elif type_bits == 0b01:
            if pos + 1 >= len(data):
                return None, pos
            length = ((first & 0x3F) << 8) | data[pos + 1]
            return length, pos + 2
        elif type_bits == 0b10:
            if pos + 4 >= len(data):
                return None, pos
            length = int.from_bytes(data[pos + 1:pos + 5], "big")
            return length, pos + 5
        else:
            # 0b11: special string encoding, return the type bits
            return (first & 0x3F), pos

    def read_string_encoded(data, pos):
        """Read a string-encoded value. Returns (string_bytes, new_pos)."""
        if pos >= len(data):
            return None, pos
        first = data[pos]
        type_bits = (first >> 6) & 0b11
        if type_bits == 0b11:
            special_type = first & 0x3F
            if special_type == 0:  # 0xC0: 8-bit integer
                if pos + 1 >= len(data):
                    return None, pos
                val = data[pos + 1]
                return str(val).encode(), pos + 2
            elif special_type == 1:  # 0xC1: 16-bit integer
                if pos + 2 >= len(data):
                    return None, pos
                val = int.from_bytes(data[pos + 1:pos + 3], "little")
                return str(val).encode(), pos + 3
            elif special_type == 2:  # 0xC2: 32-bit integer
                if pos + 4 >= len(data):
                    return None, pos
                val = int.from_bytes(data[pos + 1:pos + 5], "little")
                return str(val).encode(), pos + 5
            elif special_type == 3:  # 0xC3: LZF compressed (not needed)
                return None, pos
        else:
            length, pos = read_length(data, pos)
            if length is None or pos + length > len(data):
                return None, pos
            return data[pos:pos + length], pos + length

    while pos < len(data):
        byte = data[pos]
        if byte == 0xFF:  # End of file
            break
        elif byte == 0xFA:  # Metadata subsection
            pos += 1
            # Skip metadata name and value (both string encoded)
            _, pos = read_string_encoded(data, pos)
            _, pos = read_string_encoded(data, pos)
        elif byte == 0xFE:  # Database subsection
            pos += 1
            # Database index (size encoded)
            _, pos = read_length(data, pos)
        elif byte == 0xFB:  # Hash table size info
            pos += 1
            # Hash table size (size encoded)
            _, pos = read_length(data, pos)
            # Expire hash table size (size encoded)
            _, pos = read_length(data, pos)
        elif byte == 0xFC:  # Expire in milliseconds
            pos += 1
            if pos + 8 > len(data):
                break
            expires_ms = int.from_bytes(data[pos:pos + 8], "little")
            pos += 8
            # Value type byte follows
            if pos >= len(data):
                break
            value_type = data[pos]
            pos += 1
            if value_type == 0x00:  # String type
                key, pos = read_string_encoded(data, pos)
                value, pos = read_string_encoded(data, pos)
                if key is not None and value is not None:
                    # Convert ms timestamp to monotonic deadline
                    deadline = time.monotonic() + (expires_ms - time.time() * 1000) / 1000.0
                    with store_lock:
                        store[key] = (value, deadline)
        elif byte == 0xFD:  # Expire in seconds
            pos += 1
            if pos + 4 > len(data):
                break
            expires_s = int.from_bytes(data[pos:pos + 4], "little")
            pos += 4
            if pos >= len(data):
                break
            value_type = data[pos]
            pos += 1
            if value_type == 0x00:
                key, pos = read_string_encoded(data, pos)
                value, pos = read_string_encoded(data, pos)
                if key is not None and value is not None:
                    deadline = time.monotonic() + (expires_s * 1000 - time.time() * 1000) / 1000.0
                    with store_lock:
                        store[key] = (value, deadline)
        elif byte == 0x00:  # String value type (no expiry)
            pos += 1
            key, pos = read_string_encoded(data, pos)
            value, pos = read_string_encoded(data, pos)
            if key is not None and value is not None:
                with store_lock:
                    store[key] = (value, None)
        else:
            # Unknown byte, skip (shouldn't happen in well-formed RDB)
            pos += 1


class _Watcher:
    """Per-connection WATCH registration (hashable, so it can live in
    the watched_keys sets)."""

    def __init__(self):
        self.keys = set()
        self.dirty = False


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
    served_any = False
    while queue and lst:
        waiter = queue.popleft()
        waiter.key = key
        waiter.value = lst.pop(0)
        waiter.served = True
        waiter.event.set()
        served_any = True
    if served_any:
        mark_key_dirty(key)


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


def mark_key_dirty(key):
    """Flag every WATCH registration on `key` as modified, so the
    watching transactions abort at EXEC time."""
    with watched_lock:
        for watcher in watched_keys.get(key, ()):
            watcher.dirty = True


def unwatch_tx(tx):
    """Drop all WATCH registrations held by a connection and reset its
    dirty flag."""
    watcher = tx.get("watcher")
    if watcher is None:
        return
    with watched_lock:
        for key in watcher.keys:
            watchers = watched_keys.get(key)
            if watchers is not None:
                watchers.discard(watcher)
                if not watchers:
                    del watched_keys[key]
    watcher.keys = set()
    watcher.dirty = False


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


# Write commands that should be propagated to replicas.
WRITE_COMMANDS = {"set", "del", "lpush", "rpush", "lpop", "rpop",
                  "incr", "decr", "incrby", "decrby", "zadd", "geoadd"}


def append_to_aof(args):
    """Append a command in RESP format to the active AOF file."""
    if aof_file_path is None or config_appendonly != "yes":
        return
    resp = encode_resp_array(args)
    with open(aof_file_path, "ab") as f:
        f.write(resp)
        if config_appendfsync == "always":
            f.flush()
            os.fsync(f.fileno())


def propagate_to_replicas(args):
    """Send a command to all connected replicas as a RESP array."""
    global master_repl_offset
    with replica_connections_lock:
        replicas = list(replica_connections)
    payload = encode_resp_array(args)
    for replica_conn in replicas:
        try:
            replica_conn.sendall(payload)
        except (ConnectionResetError, BrokenPipeError):
            pass  # replica disconnected; cleanup handled elsewhere
    master_repl_offset += len(payload)


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
        # Abort if any watched key was touched since WATCH; watch state
        # is cleared whether the transaction succeeds or aborts.
        watcher = tx.get("watcher")
        aborted = bool(watcher and watcher.dirty)
        unwatch_tx(tx)
        queued = tx["queue"]
        tx["queue"] = []
        if aborted:
            return b"*-1\r\n"
        # Execute every queued command; EXEC replies with an array of
        # their responses (empty array when nothing was queued).
        responses = [execute_command(cmd, tx) for cmd in queued]
        return b"*" + str(len(responses)).encode() + b"\r\n" + b"".join(responses)
    if command == "discard":
        # Must be handled before queueing so it works mid-transaction.
        if tx is None or not tx.get("active"):
            return b"-ERR DISCARD without MULTI\r\n"
        tx["active"] = False
        tx["queue"] = []
        unwatch_tx(tx)  # DISCARD also flushes watched keys, like Redis
        return b"+OK\r\n"
    if command == "watch" and len(args) >= 2:
        # Optimistic locking: WATCH is only allowed outside a transaction.
        if tx is not None and tx.get("active"):
            return b"-ERR WATCH inside MULTI is not allowed\r\n"
        if tx is not None:
            watcher = tx.setdefault("watcher", _Watcher())
            with watched_lock:
                for key in args[1:]:
                    watcher.keys.add(key)
                    watched_keys.setdefault(key, set()).add(watcher)
        return b"+OK\r\n"
    if command == "unwatch":
        # Flush every watched key for this connection; always +OK.
        if tx is not None:
            unwatch_tx(tx)
        return b"+OK\r\n"
    # A transaction is active: queue every other command instead of
    # executing it, so the database stays untouched until EXEC.
    if tx is not None and tx.get("active"):
        tx["queue"].append(args)
        return b"+QUEUED\r\n"
    if command == "ping":
        return b"+PONG\r\n"
    if command == "replconf":
        # Handle REPLCONF GETACK: respond with REPLCONF ACK <offset>
        if len(args) >= 2 and args[1].decode("utf-8", "replace").lower() == "getack":
            return encode_resp_array([b"REPLCONF", b"ACK", b"0"])
        return b"+OK\r\n"
    if command == "psync":
        repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        return f"+FULLRESYNC {repl_id} 0\r\n".encode()
    if command == "wait" and len(args) >= 3:
        num_replicas_needed = int(args[1])
        timeout_ms = int(args[2])
        with replica_connections_lock:
            replicas = list(replica_connections)
        if not replicas:
            return b":0\r\n"
        # Send REPLCONF GETACK * to all replicas
        getack_cmd = encode_resp_array([b"REPLCONF", b"GETACK", b"*"])
        for replica_conn in replicas:
            try:
                replica_conn.sendall(getack_cmd)
            except (ConnectionResetError, BrokenPipeError):
                pass
        # Poll until timeout or enough replicas have caught up
        deadline = time.monotonic() + timeout_ms / 1000.0
        while True:
            acked = 0
            with replica_ack_lock:
                for replica_conn in replicas:
                    offset = replica_ack_offsets.get(id(replica_conn))
                    # A replica is considered caught up if:
                    # 1. It has acknowledged and its offset >= master_repl_offset, OR
                    # 2. master_repl_offset is 0 (no writes) — replicas that haven't
                    #    yet responded are still at offset 0 by default.
                    if offset is not None and offset >= master_repl_offset:
                        acked += 1
                    elif master_repl_offset == 0:
                        acked += 1
            if acked >= num_replicas_needed or time.monotonic() >= deadline:
                return b":" + str(acked).encode() + b"\r\n"
            time.sleep(0.01)
    if command == "echo":
        value = args[1] if len(args) > 1 else b""
        return encode_bulk_string(value)
    if command == "info":
        section = args[1].decode("utf-8", "replace").lower() if len(args) > 1 else ""
        if section == "replication" or section == "":
            lines = [
                f"role:{server_role}",
                "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
                "master_repl_offset:0",
            ]
            return encode_bulk_string("\r\n".join(lines).encode())
    if command == "config" and len(args) >= 3:
        sub = args[1].decode("utf-8", "replace").lower()
        if sub == "get":
            param = args[2].decode("utf-8", "replace").lower()
            value = ""
            if param == "dir":
                value = config_dir
            elif param == "dbfilename":
                value = config_dbfilename
            elif param == "appendonly":
                value = config_appendonly
            elif param == "appenddirname":
                value = config_appenddirname
            elif param == "appendfilename":
                value = config_appendfilename
            elif param == "appendfsync":
                value = config_appendfsync
            return encode_resp_array([param.encode(), value.encode()])
    if command == "keys" and len(args) >= 2:
        pattern = args[1].decode("utf-8", "replace")
        if pattern == "*":
            with store_lock:
                keys = list(store.keys())
            return encode_resp_array(keys)
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
        mark_key_dirty(args[1])
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
                mark_key_dirty(key)
                return b":1\r\n"
            value, expires_at = entry
            try:
                current = int(value)
            except ValueError:
                return b"-ERR value is not an integer or out of range\r\n"
            new_value = current + 1
            # Preserve any existing expiry, like real Redis.
            store[key] = (str(new_value).encode(), expires_at)
            mark_key_dirty(key)
            return b":" + str(new_value).encode() + b"\r\n"
    if command == "rpush" and len(args) >= 3:
        key, values = args[1], args[2:]
        with lists_lock:
            lst = lists.setdefault(key, [])
            lst.extend(values)
            length = len(lst)
            serve_blpop_waiters(key)
        mark_key_dirty(key)
        return b":" + str(length).encode() + b"\r\n"
    if command == "zadd" and len(args) >= 4:
        key = args[1]
        # ZADD key score member [score member ...]
        added = 0
        with sorted_sets_lock:
            zset = sorted_sets.setdefault(key, [])
            # Build a set of existing members for quick lookup
            existing = {m: s for s, m in zset}
            i = 2
            while i + 1 < len(args):
                score = float(args[i])
                member = args[i + 1]
                if member in existing:
                    # Update score of existing member
                    existing[member] = score
                else:
                    existing[member] = score
                    added += 1
                i += 2
            # Rebuild zset from existing dict (handles score updates) and sort
            sorted_sets[key] = [(s, m) for m, s in sorted(existing.items(), key=lambda x: (x[1], x[0]))]
        mark_key_dirty(key)
        return b":" + str(added).encode() + b"\r\n"
    if command == "zrank" and len(args) >= 3:
        key = args[1]
        member = args[2]
        with sorted_sets_lock:
            zset = sorted_sets.get(key)
            if zset is None:
                return b"$-1\r\n"
            # zset is sorted by (score, member), so index = rank
            for i, (s, m) in enumerate(zset):
                if m == member:
                    return b":" + str(i).encode() + b"\r\n"
            return b"$-1\r\n"
    if command == "zcard" and len(args) >= 2:
        key = args[1]
        with sorted_sets_lock:
            zset = sorted_sets.get(key)
            return b":" + str(len(zset) if zset else 0).encode() + b"\r\n"
    if command == "zscore" and len(args) >= 3:
        key = args[1]
        member = args[2]
        with sorted_sets_lock:
            zset = sorted_sets.get(key)
            if zset is None:
                return b"$-1\r\n"
            for s, m in zset:
                if m == member:
                    return encode_bulk_string(str(s).encode())
            return b"$-1\r\n"
    if command == "zrem" and len(args) >= 3:
        key = args[1]
        member = args[2]
        removed = 0
        with sorted_sets_lock:
            zset = sorted_sets.get(key)
            if zset is not None:
                for i, (s, m) in enumerate(zset):
                    if m == member:
                        zset.pop(i)
                        removed = 1
                        break
        mark_key_dirty(key)
        return b":" + str(removed).encode() + b"\r\n"
    if command == "geoadd" and len(args) >= 5:
        key = args[1]
        lon = float(args[2])
        lat = float(args[3])
        member = args[4]
        # Validate longitude [-180, 180] and latitude [-85.05112878, 85.05112878]
        if lon < -180 or lon > 180:
            return b"-ERR invalid longitude,latitude pair\r\n"
        if lat < -85.05112878 or lat > 85.05112878:
            return b"-ERR invalid longitude,latitude pair\r\n"
        # Encode lon/lat into a geohash score (interleaved bits)
        def spread(v):
            v &= 0xFFFFFFFF
            v = (v | (v << 16)) & 0x0000FFFF0000FFFF
            v = (v | (v << 8)) & 0x00FF00FF00FF00FF
            v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F
            v = (v | (v << 2)) & 0x3333333333333333
            v = (v | (v << 1)) & 0x5555555555555555
            return v
        def geo_score(lon, lat):
            MIN_LAT = -85.05112878
            MAX_LAT = 85.05112878
            MIN_LON = -180.0
            MAX_LON = 180.0
            LAT_RANGE = MAX_LAT - MIN_LAT
            LON_RANGE = MAX_LON - MIN_LON
            lat_norm = int((1 << 26) * (lat - MIN_LAT) / LAT_RANGE)
            lon_norm = int((1 << 26) * (lon - MIN_LON) / LON_RANGE)
            return float(spread(lat_norm) | (spread(lon_norm) << 1))
        score = geo_score(lon, lat)
        added = 0
        with sorted_sets_lock:
            zset = sorted_sets.setdefault(key, [])
            existing = {m: s for s, m in zset}
            if member in existing:
                # Update score
                existing[member] = score
                sorted_sets[key] = [(s, m) for m, s in sorted(existing.items(), key=lambda x: (x[1], x[0]))]
            else:
                zset.append((score, member))
                zset.sort(key=lambda x: (x[0], x[1]))
                added = 1
        mark_key_dirty(key)
        return b":" + str(added).encode() + b"\r\n"
    if command == "geopos" and len(args) >= 3:
        key = args[1]
        members = args[2:]
        with sorted_sets_lock:
            zset = sorted_sets.get(key)
            existing = {m: s for s, m in zset} if zset else {}
        parts = []
        for member in members:
            if member in existing:
                lon, lat = geo_decode(existing[member])
                lon_str = str(lon)
                lat_str = str(lat)
                lon_b = lon_str.encode()
                lat_b = lat_str.encode()
                parts.append(
                    b"*2\r\n$" + str(len(lon_b)).encode() + b"\r\n" + lon_b + b"\r\n"
                    + b"$" + str(len(lat_b)).encode() + b"\r\n" + lat_b + b"\r\n"
                )
            else:
                parts.append(b"*-1\r\n")
        return b"*" + str(len(members)).encode() + b"\r\n" + b"".join(parts)
    if command == "zrange" and len(args) >= 4:
        key = args[1]
        start = int(args[2])
        stop = int(args[3])
        with sorted_sets_lock:
            zset = sorted_sets.get(key)
            if zset is None:
                return b"*0\r\n"
            n = len(zset)
            # Convert negative indexes: if abs(neg) >= cardinality, treat as 0.
            if start < 0:
                start = max(0, n + start)
            if stop < 0:
                stop = max(0, n + stop)
            if start >= n or start > stop:
                return b"*0\r\n"
            end = min(stop, n - 1)
            members = [m for _, m in zset[start:end + 1]]
            return encode_resp_array(members)
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
        mark_key_dirty(key)
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
            mark_key_dirty(args[1])
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
                    mark_key_dirty(key)
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
        mark_key_dirty(key)
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
    tx = {"active": False, "queue": [], "watcher": _Watcher()}
    subscribed = False  # whether this connection is in subscribed mode
    with conn:
        buffer = b""
        is_replica = False
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
                    command = args[0].decode("utf-8", "replace").lower() if args else ""
                    # If this is a replica sending REPLCONF ACK, store the offset
                    if is_replica and command == "replconf" and len(args) >= 3:
                        sub = args[1].decode("utf-8", "replace").lower()
                        if sub == "ack":
                            try:
                                ack_offset = int(args[2])
                            except ValueError:
                                ack_offset = 0
                            with replica_ack_lock:
                                replica_ack_offsets[id(conn)] = ack_offset
                            continue  # don't send a response back
                    # Handle SUBSCRIBE: register the connection for channels
                    if command == "subscribe":
                        subscribed = True
                        for i in range(1, len(args)):
                            ch = args[i]
                            with channels_lock:
                                channels_subscribers.setdefault(ch, set()).add(conn)
                            # Respond with ["subscribe", channel, total_subscriptions_for_this_client]
                            with channels_lock:
                                client_count = sum(
                                    1 for subs in channels_subscribers.values()
                                    if conn in subs
                                )
                            resp = (
                                b"*3\r\n"
                                + b"$9\r\nsubscribe\r\n"
                                + b"$" + str(len(ch)).encode() + b"\r\n" + ch + b"\r\n"
                                + b":" + str(client_count).encode() + b"\r\n"
                            )
                            conn.sendall(resp)
                        continue  # skip normal execute_command
                    # In subscribed mode, only allow a subset of commands
                    if subscribed and command not in (
                        "subscribe", "unsubscribe", "psubscribe",
                        "punsubscribe", "ping", "quit", "reset",
                    ):
                        err_msg = f"ERR Can't execute '{command}' only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
                        conn.sendall(f"-{err_msg}\r\n".encode())
                        continue
                    # PING in subscribed mode returns ["pong", ""] instead of +PONG
                    if subscribed and command == "ping":
                        conn.sendall(b"*2\r\n$4\r\npong\r\n$0\r\n\r\n")
                        continue
                    # UNSUBSCRIBE: remove client from one or more channels
                    if command == "unsubscribe":
                        channels_to_unsub = args[1:] if len(args) > 1 else []
                        for ch in channels_to_unsub:
                            with channels_lock:
                                subs = channels_subscribers.get(ch)
                                if subs:
                                    subs.discard(conn)
                            # Count remaining subscriptions for this client
                            with channels_lock:
                                remaining = sum(
                                    1 for s in channels_subscribers.values()
                                    if conn in s
                                )
                            resp = (
                                b"*3\r\n"
                                + b"$11\r\nunsubscribe\r\n"
                                + b"$" + str(len(ch)).encode() + b"\r\n" + ch + b"\r\n"
                                + b":" + str(remaining).encode() + b"\r\n"
                            )
                            conn.sendall(resp)
                        continue
                    # PUBLISH delivers message to all subscribers and returns count
                    if command == "publish" and len(args) >= 3:
                        ch = args[1]
                        msg = args[2]
                        with channels_lock:
                            subscribers = list(channels_subscribers.get(ch, set()))
                        # Deliver ["message", channel, msg] to each subscriber
                        deliver = (
                            b"*3\r\n"
                            + b"$7\r\nmessage\r\n"
                            + b"$" + str(len(ch)).encode() + b"\r\n" + ch + b"\r\n"
                            + b"$" + str(len(msg)).encode() + b"\r\n" + msg + b"\r\n"
                        )
                        for sub_conn in subscribers:
                            try:
                                sub_conn.sendall(deliver)
                            except (ConnectionResetError, BrokenPipeError):
                                pass
                        conn.sendall(b":" + str(len(subscribers)).encode() + b"\r\n")
                        continue
                    response = execute_command(args, tx)
                    conn.sendall(response)
                    # After PSYNC, send the empty RDB file
                    if command == "psync":
                        rdb_payload = (
                            b"$" + str(len(EMPTY_RDB)).encode() + b"\r\n" + EMPTY_RDB
                        )
                        conn.sendall(rdb_payload)
                        # This connection is now a replica — register it
                        is_replica = True
                        with replica_connections_lock:
                            replica_connections.append(conn)
                    # Propagate write commands to replicas
                    elif command in WRITE_COMMANDS and is_replica is False:
                        propagate_to_replicas(args)
                        append_to_aof(args)
        except (ConnectionResetError, BrokenPipeError):
            pass  # client disconnected abruptly
        finally:
            if is_replica:
                with replica_connections_lock:
                    try:
                        replica_connections.remove(conn)
                    except ValueError:
                        pass
                with replica_ack_lock:
                    replica_ack_offsets.pop(id(conn), None)
            # Remove from all channel subscriptions
            with channels_lock:
                for subs in channels_subscribers.values():
                    subs.discard(conn)
            unwatch_tx(tx)  # drop WATCH registrations held by this connection


def handshake_with_master(master_host: str, master_port: int, replica_port: int):
    """Connect to the master and perform the initial replication handshake."""
    master_conn = socket.create_connection((master_host, master_port))
    # Step 1: Send PING as a RESP array
    master_conn.sendall(b"*1\r\n$4\r\nPING\r\n")
    response = master_conn.recv(1024)
    print(f"Master responded to PING: {response}")

    # Step 2: Send REPLCONF listening-port <PORT>
    port_str = str(replica_port).encode()
    replconf_port = (
        b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n"
        + b"$" + str(len(port_str)).encode() + b"\r\n" + port_str + b"\r\n"
    )
    master_conn.sendall(replconf_port)
    response = master_conn.recv(1024)
    print(f"Master responded to REPLCONF listening-port: {response}")

    # Step 2b: Send REPLCONF capa psync2
    master_conn.sendall(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
    response = master_conn.recv(1024)
    print(f"Master responded to REPLCONF capa: {response}")

    # Step 3: Send PSYNC ? -1
    master_conn.sendall(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
    response = master_conn.recv(1024)
    print(f"Master responded to PSYNC: {response}")

    # The recv above may have included the RDB file (and even commands) beyond
    # the +FULLRESYNC\r\n line.  Strip the FULLRESYNC response and keep the rest.
    fullresync_end = response.find(b"\r\n")
    buffer = response[fullresync_end + 2:] if fullresync_end != -1 else b""

    # After handshake, read and process commands from the master (no responses sent)
    rdb_remaining = -1  # bytes of RDB payload still to read (-1 = not in RDB)
    repl_offset = 0  # total bytes of commands processed so far
    while True:
        try:
            # Process whatever is in the buffer first
            progress = True
            while progress:
                progress = False
                # If we're inside an RDB payload, skip those bytes first
                if rdb_remaining >= 0:
                    if len(buffer) <= rdb_remaining:
                        rdb_remaining -= len(buffer)
                        buffer = b""
                    else:
                        buffer = buffer[rdb_remaining:]
                        rdb_remaining = -1
                    progress = True
                    continue
                # Check if buffer starts with an RDB bulk string ($<len>\r\n<contents>)
                if buffer.startswith(b"$"):
                    line_end = buffer.find(b"\r\n")
                    if line_end == -1:
                        break  # need more data
                    try:
                        rdb_len = int(buffer[1:line_end])
                    except ValueError:
                        break
                    payload_start = line_end + 2
                    if len(buffer) < payload_start + rdb_len:
                        rdb_remaining = rdb_len - (len(buffer) - payload_start)
                        buffer = b""
                    else:
                        buffer = buffer[payload_start + rdb_len:]
                    progress = True
                    continue
                args, buffer = parse_resp_array(buffer)
                if args is None:
                    break
                command = args[0].decode("utf-8", "replace").lower() if args else ""
                if command == "replconf" and len(args) >= 2:
                    sub = args[1].decode("utf-8", "replace").lower()
                    if sub == "getack":
                        # Respond with REPLCONF ACK <offset>
                        ack_resp = encode_resp_array(
                            [b"REPLCONF", b"ACK", str(repl_offset).encode()]
                        )
                        master_conn.sendall(ack_resp)
                else:
                    execute_command(args)
                # Add this command's full RESP byte length to the offset
                repl_offset += len(encode_resp_array(args))
                # Don't send any response back to the master
                progress = True
            # Buffer exhausted — wait for more data from master
            data = master_conn.recv(1024)
            if not data:
                break
            buffer += data
        except (ConnectionResetError, BrokenPipeError):
            break

    master_conn.close()


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--replicaof", type=str, default=None)
    parser.add_argument("--dir", type=str, default=os.getcwd())
    parser.add_argument("--dbfilename", type=str, default="dump.rdb")
    parser.add_argument("--appendonly", type=str, default="no")
    parser.add_argument("--appenddirname", type=str, default="appendonlydir")
    parser.add_argument("--appendfilename", type=str, default="appendonly.aof")
    parser.add_argument("--appendfsync", type=str, default="everysec")
    args = parser.parse_args()

    global server_role, config_dir, config_dbfilename, config_appendonly, config_appenddirname, config_appendfilename, config_appendfsync
    config_dir = args.dir
    config_dbfilename = args.dbfilename
    config_appendonly = args.appendonly
    config_appenddirname = args.appenddirname
    config_appendfilename = args.appendfilename
    config_appendfsync = args.appendfsync
    # Load RDB file on startup
    rdb_path = os.path.join(config_dir, config_dbfilename)
    load_rdb_file(rdb_path)
    # Create append-only directory, AOF file, and manifest when AOF persistence is enabled
    if config_appendonly == "yes":
        aof_dir = os.path.join(config_dir, config_appenddirname)
        os.makedirs(aof_dir, exist_ok=True)
        # Only create default AOF file and manifest if they don't already exist
        aof_name = f"{config_appendfilename}.1.incr.aof"
        aof_path = os.path.join(aof_dir, aof_name)
        if not os.path.exists(aof_path):
            with open(aof_path, "w") as f:
                pass
        manifest_path = os.path.join(aof_dir, f"{config_appendfilename}.manifest")
        if not os.path.exists(manifest_path):
            with open(manifest_path, "w") as f:
                f.write(f"file {aof_name} seq 1 type i\n")
        # Read manifest to find the active incremental AOF file
        global aof_file_path
        if os.path.exists(manifest_path):
            with open(manifest_path, "r") as f:
                for line in f:
                    parts = line.strip().split()
                    if len(parts) >= 6 and parts[0] == "file" and parts[4] == "type" and parts[5] == "i":
                        aof_file_path = os.path.join(aof_dir, parts[1])
                        break
        # Replay the AOF file to restore state
        if aof_file_path and os.path.exists(aof_file_path):
            with open(aof_file_path, "rb") as f:
                aof_data = f.read()
            buf = aof_data
            while buf:
                cmd_args, buf = parse_resp_array(buf)
                if cmd_args is None:
                    break
                try:
                    execute_command(cmd_args)
                except Exception:
                    pass  # skip malformed commands
    if args.replicaof is not None:
        server_role = "slave"
        # Parse "host port" and connect to the master
        parts = args.replicaof.split()
        master_host, master_port = parts[0], int(parts[1])
        threading.Thread(
            target=handshake_with_master,
            args=(master_host, master_port, args.port),
            daemon=True,
        ).start()

    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", args.port), reuse_port=True)

    while True:
        conn, _addr = server_socket.accept()  # wait for client
        threading.Thread(target=handle_connection, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()
