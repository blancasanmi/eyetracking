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
import copy
import time
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

CALIB_POINTS: list[tuple[float, float]] = [
    (0.5,  0.1),
    (0.05, 0.5), (0.5, 0.5), (0.95, 0.5),
    (0.5,  0.9),
]

MAX_CALIB_ATTEMPTS   = 2
CALIB_ERROR_THRESH   = 45.0   # accept calibration below this average error
CALIB_POINT_TIMEOUT  = 15.0  # seconds to wait per calibration point
CALIB_RESULT_TIMEOUT = 10.0  # seconds to wait for final result after last point

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_rec_dict(tracker: OpenGazeTracker) -> dict | None:
    """Return a snapshot of the latest REC sample, or None."""
    with tracker._inlock:
        if "REC" not in tracker._incoming:
            return None
        rec = tracker._incoming["REC"].get("NO_ID")
        if rec is None:
            return None
        return copy.deepcopy(rec)


async def _wait_for_calibration_result(
    tracker: OpenGazeTracker,
    timeout: float = CALIB_RESULT_TIMEOUT,
) -> list[dict] | None:
    """
    Poll get_calibration_result() until the server's <CAL ID="CALIB_RESULT"/>
    message has been parsed into tracker._incoming, or until timeout.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = tracker.get_calibration_result()
        if result is not None:
            return result
        await asyncio.sleep(0.1)
    return None


async def _run_calibration(tracker: OpenGazeTracker) -> tuple[float, int]:
    """
    Run one full calibration pass:
      1. Reset + add points
      2. Show window + start calibration
      3. Wait for every point to start (confirms the sequence is live)
      4. Wait for CALIB_RESULT to arrive  (server signals completion)
      5. Hide window + stop calibration
      6. Return (avg_error, valid_points)

    Raises TimeoutError if any point or the final result times out.

    Why this order matters
    ----------------------
    calibrate_start() / calibrate_show() are acknowledged immediately by the
    server, but the actual measurement runs asynchronously.  Closing the
    window before the result arrives corrupts the session.  We therefore:
      - use wait_for_calibration_point_start() (run in a thread executor so it
        doesn't block asyncio) to confirm each point has actually started, and
      - only then poll for the CALIB_RESULT before hiding the window.
    """
    # Clear any stale result so we don't read it as the new one.
    tracker.clear_calibration_result()
    tracker.calibrate_reset()

    for x, y in CALIB_POINTS:
        tracker.calibrate_addpoint(x, y)

    tracker.calibrate_show(True)
    tracker.calibrate_start(True)
    print("[GP] Calibration started")

    # wait_for_calibration_point_start() is a blocking call (uses time.sleep
    # internally), so run it in a thread pool to keep the event loop alive.
    loop = asyncio.get_running_loop()

    for i, _ in enumerate(CALIB_POINTS, start=1):
        pt_nr, pos = await loop.run_in_executor(
            None,
            lambda: tracker.wait_for_calibration_point_start(
                timeout=CALIB_POINT_TIMEOUT
            ),
        )
        if pt_nr is None:
            # Safe to abort: hide window before raising.
            tracker.calibrate_show(False)
            tracker.calibrate_start(False)
            raise TimeoutError(
                f"Timed out waiting for calibration point {i} to start."
            )
        print(f"  [CALIB] Point {pt_nr} at {pos}")

    # All points collected — now wait for the server to send CALIB_RESULT.
    print("[GP] All points collected — waiting for calibration result...")
    result = await _wait_for_calibration_result(tracker)

    # Only close the window once we have (or have given up waiting for) the result.
    tracker.calibrate_show(False)
    tracker.calibrate_start(False)

    if result is None:
        raise TimeoutError(
            "All calibration points completed but no result arrived in time."
        )

    avg_error_str, valid_points_str = tracker.calibrate_result_summary()
    avg_error    = float(avg_error_str)         if avg_error_str    is not None else 9999.0
    valid_points = int(float(valid_points_str)) if valid_points_str is not None else 0

    print(f"[GP] Calibration result: avg_error={avg_error:.4f}  valid_points={valid_points}")
    return avg_error, valid_points


# ---------------------------------------------------------------------------
# WebSocket handler
# ---------------------------------------------------------------------------

async def handler(ws: websockets.WebSocketServerProtocol) -> None:
    print("[WS] Browser connected")

    tracker = OpenGazeTracker(ip=GP_HOST, port=GP_PORT, logfile=LOGFILE)
    print(f"[GP] OpenGazeTracker connected to {GP_HOST}:{GP_PORT}")

    stop_event = asyncio.Event()
    sample_count = 0

    # ------------------------------------------------------------------
    # Task: forward gaze samples to the browser
    # ------------------------------------------------------------------
    async def forward_gaze() -> None:
        nonlocal sample_count
        while not stop_event.is_set():
            rec = _build_rec_dict(tracker)
            if rec is not None:
                try:
                    await ws.send(json.dumps({"type": "gaze", "data": rec}))
                    sample_count += 1
                    if sample_count % 5000 == 0:
                        print(f"  [GP] {sample_count} gaze samples forwarded")
                except websockets.ConnectionClosed:
                    stop_event.set()
                    return
            await asyncio.sleep(0.005)   # ~200 Hz ceiling

    # ------------------------------------------------------------------
    # Task: receive commands from the browser
    # ------------------------------------------------------------------
    async def receive_commands() -> None:
        try:
            async for raw in ws:
                msg = json.loads(raw)
                cmd = msg.get("cmd")

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
                                "type":     "calibration_failed",
                                "reason":   "max_attempts",
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
                                "detail": str(exc),
                            }))
                            break
                    else:
                        # while…else: loop exited because avg_error <= threshold.
                        tracker.start_recording()
                        await ws.send(json.dumps({
                            "type":         "calibration_done",
                            "avg_error":    avg_error,
                            "valid_points": valid_points,
                            "attempts":     attempts,
                        }))

                elif cmd == "stop":
                    tracker.stop_recording()
                    stop_event.set()
                    break

        except websockets.ConnectionClosed:
            stop_event.set()

    # ------------------------------------------------------------------
    # Run both tasks concurrently; clean up on exit
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
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())