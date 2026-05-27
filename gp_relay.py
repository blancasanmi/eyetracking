"""
GazePoint WebSocket Relay (v3)
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

CALIBRATION_POINTS = 9  # 9-point grid for better accuracy on reading tasks


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


def run_calibration(sock):
    """
    Run Gazepoint calibration with 9 points and return quality result.
    Returns a dict with calibration quality info or None on timeout.
    """
    print(f"[GP] Starting {CALIBRATION_POINTS}-point calibration...")

    # Reset any previous calibration
    sock.sendall(b'<SET ID="CALIBRATE_RESET" STATE="1" />\r\n')

    # Set number of calibration points
    sock.sendall(f'<SET ID="CALIBRATE_NUMPOINTS" VALUE="{CALIBRATION_POINTS}" />\r\n'.encode())

    # Start calibration
    sock.sendall(b'<SET ID="CALIBRATE_START" STATE="1" />\r\n')

    # Wait for calibration result (timeout after 120s)
    buf = ""
    timeout_count = 0
    max_timeout = 1200  # 120 seconds (each loop = 0.1s)

    while timeout_count < max_timeout:
        try:
            chunk = sock.recv(8192).decode("utf-8", errors="ignore")
            buf += chunk

            # Gazepoint signals completion with CALIBRATE_RESULT
            if "CALIBRATE_RESULT" in buf:
                print("[GP] Calibration complete, parsing result...")
                try:
                    # Extract calibration result XML
                    match = re.search(r'<ACK[^>]*CALIBRATE_RESULT[^>]*/>', buf)
                    if match:
                        el = ET.fromstring(match.group())
                        result = dict(el.attrib)
                        avg_error = float(result.get("AVE", -1))
                        print(f"[GP] Average calibration error: {avg_error:.4f} degrees")
                        return {
                            "success": True,
                            "avg_error": avg_error,
                            "quality": _rate_calibration(avg_error),
                            "raw": result
                        }
                except Exception as e:
                    print(f"[GP] Could not parse calibration result: {e}")
                    return {"success": True, "avg_error": -1, "quality": "unknown"}

        except socket.timeout:
            timeout_count += 1
            continue
        except OSError:
            break

    print("[GP] Calibration timed out")
    return None


def _rate_calibration(avg_error_degrees):
    """Rate calibration quality based on average error in degrees."""
    if avg_error_degrees < 0:
        return "unknown"
    elif avg_error_degrees < 0.5:
        return "excellent"
    elif avg_error_degrees < 1.0:
        return "good"
    elif avg_error_degrees < 2.0:
        return "acceptable"
    else:
        return "poor"


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
                    # Run calibration in a thread so we don't block the event loop
                    loop = asyncio.get_event_loop()
                    result = await loop.run_in_executor(None, run_calibration, sock)

                    if result:
                        print(f"[GP] Calibration quality: {result['quality']} "
                              f"(avg error: {result['avg_error']:.4f} deg)")
                        await ws.send(json.dumps({
                            "type": "calibration_result",
                            "quality": result["quality"],
                            "avg_error": result["avg_error"],
                            "success": result["success"]
                        }))
                    else:
                        await ws.send(json.dumps({
                            "type": "calibration_result",
                            "quality": "unknown",
                            "avg_error": -1,
                            "success": False
                        }))

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