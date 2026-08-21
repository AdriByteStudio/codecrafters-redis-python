import socket
import threading


def handle_connection(conn):
    with conn:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            # Hardcoded RESP response: +PONG\r\n (simple string "PONG")
            conn.sendall(b"+PONG\r\n")


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        conn, _addr = server_socket.accept()  # wait for client
        threading.Thread(target=handle_connection, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()
