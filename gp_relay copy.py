"""
GazePoint WebSocket Relay (v3)
==============================
Requires: pip install websockets lxml

Delegates all tracker I/O to OpenGazeTracker (opengaze.py).
No raw sockets here — the tracker class owns the connection.

Usage:
  1. Start Gazepoint Control
  2. python gp_relay.py
  3. Open experiment in browser  →  ws://localhost:8765
"""

import asyncio
import json
import queue
import time
import copy
import datetime
import websockets

from opengaze import OpenGazeTracker

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

GP_HOST   = "127.0.0.1"
GP_PORT   = 4242
WS_HOST   = "localhost"
WS_PORT   = 8765

LOGFILE   = f"gaze_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.tsv"

# Calibration points (normalised 0-1, origin = top-left)
CALIB_POINTS: list[tuple[float, float]] = [
    (0.5,  0.1),
    (0.05, 0.5), (0.5, 0.5), (0.95, 0.5),
    (0.5,  0.9),
]

MAX_CALIB_ATTEMPTS  = 3
CALIB_ERROR_THRESH  = 1.0   # average error threshold to accept calibration
CALIB_TIMEOUT_S     = 30.0  # seconds to wait for calibration to finish

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_rec_dict(tracker: OpenGazeTracker) -> dict | None:
    """
    Read the latest REC sample directly from the tracker's incoming dict.
    Returns a plain dict of string values, or None if no sample is available.
    """
    tracker._inlock.acquire()
    try:
        if "REC" not in tracker._incoming:
            return None
        rec = tracker._incoming["REC"].get("NO_ID")
        if rec is None:
            return None
        return copy.deepcopy(rec)
    finally:
        tracker._inlock.release()


async def _run_calibration(tracker: OpenGazeTracker) -> tuple[float, int]:
    """
    Run one calibration pass through CALIB_POINTS.
    Returns (avg_error, valid_points).  Raises TimeoutError on timeout.
    """
    tracker.calibrate_reset()
    for x, y in CALIB_POINTS:
        tracker.calibrate_addpoint(x, y)

    tracker.calibrate_show(True)
    tracker.calibrate_start(True)
    print("[GP] Calibration started")

    # Wait for the tracker to finish (poll for CALIB_RESULT)
    deadline = time.monotonic() + CALIB_TIMEOUT_S
    while time.monotonic() < deadline:
        result = tracker.get_calibration_result()
        if result is not None:
            break
        await asyncio.sleep(0.2)
    else:
        tracker.calibrate_show(False)
        tracker.calibrate_start(False)
        raise TimeoutError("Calibration timed out — no result received")

    tracker.calibrate_show(False)
    tracker.calibrate_start(False)

    avg_error_str, valid_points_str = tracker.calibrate_result_summary()
    avg_error    = float(avg_error_str)    if avg_error_str    is not None else 9999.0
    valid_points = int(float(valid_points_str)) if valid_points_str is not None else 0

    print(f"[GP] Calibration result: avg_error={avg_error:.4f}  valid_points={valid_points}")
    return avg_error, valid_points


# ---------------------------------------------------------------------------
# WebSocket handler (one per browser connection)
# ---------------------------------------------------------------------------

async def handler(ws: websockets.WebSocketServerProtocol) -> None:
    print("[WS] Browser connected")

    # One shared tracker per handler session.
    # (Move instantiation outside if you want to share across connections.)
    tracker = OpenGazeTracker(ip=GP_HOST, port=GP_PORT, logfile=LOGFILE)
    print(f"[GP] OpenGazeTracker connected to {GP_HOST}:{GP_PORT}")

    stop_event = asyncio.Event()
    sample_count = 0

    # ------------------------------------------------------------------
    # Task: forward gaze samples to browser
    # ------------------------------------------------------------------
    async def forward_gaze() -> None:
        nonlocal sample_count
        tracker.start_recording()

        while not stop_event.is_set():
            rec = _build_rec_dict(tracker)
            if rec is not None:
                try:
                    await ws.send(json.dumps({"type": "gaze", "data": rec}))
                    sample_count += 1
                    if sample_count % 500 == 0:
                        print(f"  [GP] {sample_count} gaze samples forwarded")
                except websockets.ConnectionClosed:
                    stop_event.set()
                    return

            await asyncio.sleep(0.005)   # ~200 Hz ceiling

        tracker.stop_recording()

    # ------------------------------------------------------------------
    # Task: receive commands from the browser
    # ------------------------------------------------------------------
    async def receive_commands() -> None:
        try:
            async for raw in ws:
                msg  = json.loads(raw)
                cmd  = msg.get("cmd")

                if cmd == "trigger":
                    value = str(msg.get("value", "")).replace('"', "'")
                    tracker.user_data(value)
                    print(f"  [TRIGGER] {value}")

                elif cmd == "calibrate":
                    avg_error = float("inf")
                    attempts  = 0

                    while avg_error > CALIB_ERROR_THRESH:
                        if attempts >= MAX_CALIB_ATTEMPTS:
                            print(
                                "[GP] Max calibration attempts reached. "
                                "Check eye-tracker position."
                            )
                            await ws.send(json.dumps({
                                "type":    "calibration_failed",
                                "reason":  "max_attempts",
                                "attempts": attempts,
                            }))
                            break

                        try:
                            avg_error, valid_points = await _run_calibration(tracker)
                            attempts += 1
                        except TimeoutError as exc:
                            print(f"[GP] {exc}")
                            await ws.send(json.dumps({
                                "type":   "calibration_failed",
                                "reason": "timeout",
                            }))
                            break
                    else:
                        await ws.send(json.dumps({
                            "type":         "calibration_done",
                            "avg_error":    avg_error,
                            "valid_points": valid_points,
                            "attempts":     attempts,
                        }))

                elif cmd == "stop":
                    stop_event.set()
                    break

        except websockets.ConnectionClosed:
            stop_event.set()

    # ------------------------------------------------------------------
    # Run both tasks; clean up on exit
    # ------------------------------------------------------------------
    try:
        await asyncio.gather(forward_gaze(), receive_commands())
    finally:
        stop_event.set()
        tracker.close()
        print(f"[WS] Session ended — {sample_count} total gaze samples sent")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    print(f"[WS] Relay listening on ws://{WS_HOST}:{WS_PORT}")
    print("     Waiting for browser to connect...")
    async with websockets.serve(handler, WS_HOST, WS_PORT):
        await asyncio.Future()   # run forever


if __name__ == "__main__":
    asyncio.run(main())