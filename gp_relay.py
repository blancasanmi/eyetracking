"""
GazePoint WebSocket Relay (v2)
==============================
Requires: pip install websockets

1. Start Gazepoint Control
2. python gp_relay.py
3. Open experiment in browser
"""

import asyncio
import websockets
import socket
import json
import threading
import queue
import re
import xml.etree.ElementTree as ET

GP_HOST = "127.0.0.1"
GP_PORT = 4242
WS_HOST = "localhost"
WS_PORT = 8765


def gaze_reader_thread(sock, gaze_queue, stop_event):
    """Blocking TCP reader in its own thread. Parses <REC .../> and queues dicts."""
    buf = ""
    pattern = re.compile(r'<REC\s[^>]*/>')
    while not stop_event.is_set():
        try:
            chunk = sock.recv(8192)
            if not chunk:
                break
            buf += chunk.decode("utf-8", errors="ignore")

            last_end = 0
            for m in pattern.finditer(buf):
                try:
                    el = ET.fromstring(m.group())
                    gaze_queue.put(dict(el.attrib))
                except ET.ParseError:
                    pass
                last_end = m.end()
            if last_end:
                buf = buf[last_end:]

        except socket.timeout:
            continue
        except OSError:
            break

    print("[THREAD] Gaze reader stopped")


async def handler(ws):
    print("[WS] Browser connected")

    sock = socket.create_connection((GP_HOST, GP_PORT), timeout=5)
    sock.settimeout(0.1)
    print(f"[GP] Connected to Gazepoint Control at {GP_HOST}:{GP_PORT}")

    for cmd in [
        '<SET ID="ENABLE_SEND_POG_FIX" STATE="1" />',
        '<SET ID="ENABLE_SEND_POG_BEST" STATE="1" />',
        '<SET ID="ENABLE_SEND_TIME" STATE="1" />',
        '<SET ID="ENABLE_SEND_USER_DATA" STATE="1" />',
        '<SET ID="ENABLE_SEND_DATA" STATE="1" />',
    ]:
        sock.sendall((cmd + "\r\n").encode())
        await asyncio.sleep(0.05)

    print("[GP] Data streaming enabled")

    gaze_queue = queue.Queue()
    stop_event = threading.Event()
    reader = threading.Thread(
        target=gaze_reader_thread,
        args=(sock, gaze_queue, stop_event),
        daemon=True
    )
    reader.start()

    sample_count = 0

    async def forward_gaze():
        nonlocal sample_count
        while not stop_event.is_set():
            batch = []
            try:
                while True:
                    batch.append(gaze_queue.get_nowait())
            except queue.Empty:
                pass

            for rec in batch:
                try:
                    await ws.send(json.dumps({"type": "gaze", "data": rec}))
                    sample_count += 1
                    if sample_count % 500 == 0:
                        print(f"  [GP] {sample_count} gaze samples forwarded")
                except websockets.ConnectionClosed:
                    stop_event.set()
                    return

            await asyncio.sleep(0.005)

    async def receive_commands():
        try:
            async for raw in ws:
                msg = json.loads(raw)
                cmd = msg.get("cmd")
                if cmd == "trigger":
                    value = msg.get("value", "").replace('"', "'")
                    sock.sendall(f'<SET ID="USER_DATA" VALUE="{value}" />\r\n'.encode())
                    print(f"  [TRIGGER] {value}")
                elif cmd == "calibrate":
                    points_9 = [
                    (0.1, 0.1), (0.5, 0.1), (0.9, 0.1),  
                    (0.1, 0.5), (0.5, 0.5), (0.9, 0.5),  
                    (0.1, 0.9), (0.5, 0.9), (0.9, 0.9),  
                    ]

                    sock.sendall(b'<SET ID="CALIBRATE_RESET" STATE="1" />\r\n')

                    for x, y in points_9:
                        sock.sendall(f'<SET ID="CALIBRATE_ADDPOINT" X="{x}" Y="{y}" />\r\n'.encode())
                        
                    sock.sendall(b'<SET ID="CALIBRATE_SHOW" STATE="1" />\r\n')
                    sock.sendall(b'<SET ID="CALIBRATE_START" STATE="1" />\r\n')
                    print("[GP] Calibration started")
                elif cmd == "stop":
                    stop_event.set()
                    break
        except websockets.ConnectionClosed:
            stop_event.set()

    try:
        await asyncio.gather(forward_gaze(), receive_commands())
    finally:
        stop_event.set()
        reader.join(timeout=2)
        sock.close()
        print(f"[WS] Session ended — {sample_count} total gaze samples sent")


async def main():
    print(f"[WS] Relay listening on ws://{WS_HOST}:{WS_PORT}")
    print("     Waiting for browser to connect...")
    async with websockets.serve(handler, WS_HOST, WS_PORT):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
