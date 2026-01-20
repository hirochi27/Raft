import socket
import json
import time

LEADER_PORT = 10001
TOTAL_REQUESTS = 1000
INTERVAL = 0.005  # 秒

def send_set_command(key, value):
    message = {
        "type": "CLIENT_REQUEST",
        "command": ("SET", key, value)
    }

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(("127.0.0.1", LEADER_PORT))
        sock.send(json.dumps(message).encode("utf-8"))
    except ConnectionRefusedError:
        print("Leader is not available")
    finally:
        sock.close()

if __name__ == "__main__":
    for i in range(TOTAL_REQUESTS):
        send_set_command(f"key{i}", str(i))
        print(f"sent SET key{i} {i}")
        time.sleep(INTERVAL)
