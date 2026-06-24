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
import os
import random
import time
import datetime
import websockets
import re
import csv
import tkinter as tk
from tkinter import messagebox


from opengaze import OpenGazeTracker

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

GP_HOST   = "127.0.0.1"
GP_PORT   = 4242
WS_HOST   = "localhost"
WS_PORT   = 8765

FOLDER = "data/"
LOGFILE   = f"gaze_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"

CALIB_POINTS: list[tuple[float, float]] = [
    (0.5,  0.1),
    (0.05, 0.5), (0.5, 0.5), (0.95, 0.5),
    (0.5,  0.9),
]

MAX_CALIB_ATTEMPTS   = 2
CALIB_ERROR_THRESH   = 20.0   # accept calibration below this average error
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


def load_sentences_from_js(filepath: str) -> tuple[list[str], list[str]]:
    """
    Parse sentences.js and return (sentence_first, sentence_second) as two
    lists of equal length, where sentence_first[i] / sentence_second[i] form
    a pair with original index i.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    def extract_array(var_name: str) -> list[str]:
        match = re.search(
            rf'const\s+{var_name}\s*=\s*\[(.+?)\]',
            content,
            re.DOTALL,
        )
        if not match:
            raise ValueError(f"Could not find 'const {var_name} = [...]' in {filepath}.")
        sentences = re.findall(r'"([^"]+)"', match.group(1))
        if not sentences:
            raise ValueError(f"No sentences found inside {var_name}.")
        return sentences

    first  = extract_array("SENTENCE_FIRST")
    second = extract_array("SENTENCE_SECOND")

    if len(first) != len(second):
        raise ValueError(
            f"SENTENCE_FIRST ({len(first)}) and SENTENCE_SECOND ({len(second)}) "
            "must have the same length."
        )

    # Shuffle pairs together so both arrays share the same new order
    indices = list(range(len(first)))
    random.shuffle(indices)
    first  = [first[i]  for i in indices]
    second = [second[i] for i in indices]

    with open(f"data/sentence_order_{LOGFILE}", 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['first', 'second'])
        writer.writerows(zip(first, second))

    return first, second


def catch_trial_sentences(sentences_first: list[str], sentences_second: list[str]) -> dict[int, dict]:
    catch_trials = {}

    seen = []
    # Each "full sentence" is the pair joined — unseen pool starts as all pairs
    full_sentences = [f"{s1} {s2}" for s1, s2 in zip(sentences_first, sentences_second)]
    unseen_pool = full_sentences.copy()

    i = 0
    while i < len(full_sentences):
        interval = random.randint(7, 13)
        catch_position = i + interval

        while i < catch_position and i < len(full_sentences):
            sentence = full_sentences[i]
            seen.append(sentence)
            if sentence in unseen_pool:
                unseen_pool.remove(sentence)
            i += 1

        if i >= catch_position:
            use_seen = random.random() < 0.5 and len(seen) >= 1

            if use_seen:
                recent = seen[-5:]
                catch_sentence = random.choice(recent)
                catch_type = "seen"
            else:
                if unseen_pool:
                    catch_sentence = random.choice(unseen_pool)
                    catch_type = "unseen"
                else:
                    catch_sentence = random.choice(seen)
                    catch_type = "seen"

            catch_trials[catch_position] = {
                "sentence": catch_sentence,
                "type": catch_type,
            }

    print(catch_trials)
    return catch_trials

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
    """
    tracker.clear_calibration_result()
    tracker.calibrate_reset()

    tracker.calibrate_show(True)
    tracker.calibrate_start(True)
    print("[GP] Calibration started")

    loop = asyncio.get_running_loop()

    for i, _ in enumerate(CALIB_POINTS, start=1):
        pt_nr, pos = await loop.run_in_executor(
            None,
            lambda: tracker.wait_for_calibration_point_start(
                timeout=CALIB_POINT_TIMEOUT
            ),
        )
        if pt_nr is None:
            tracker.calibrate_show(False)
            tracker.calibrate_start(False)
            raise TimeoutError(
                f"Timed out waiting for calibration point {i} to start."
            )
        print(f"  [CALIB] Point {pt_nr} at {pos}")

    print("[GP] All points collected — waiting for calibration result...")
    result = await _wait_for_calibration_result(tracker)

    await asyncio.sleep(2.0)
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

    tracker = OpenGazeTracker(ip=GP_HOST, port=GP_PORT, logfile=os.path.join(FOLDER, LOGFILE))
    print(f"[GP] OpenGazeTracker connected to {GP_HOST}:{GP_PORT}")

    # Load (and shuffle) sentences, then generate catch trials from that order
    sentences_first, sentences_second = load_sentences_from_js("sentences.js")
    catch_trials = catch_trial_sentences(sentences_first, sentences_second)

    # Send sentences and catch trials to the browser in one message
    await ws.send(json.dumps({
        "type": "init",
        "sentences_first":  sentences_first,
        "sentences_second": sentences_second,
        "catch_trials": {str(k): v for k, v in catch_trials.items()},
    }))
    print(f"[WS] Sent {len(sentences_first)} sentence pairs and {len(catch_trials)} catch trials to browser")

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
    async def run_logic():
        avg_error = float("inf")
        valid_points = 0
        attempts = 0

        for _ in range(MAX_CALIB_ATTEMPTS):
            try:
                avg_error, valid_points = await _run_calibration(tracker)
                attempts += 1
                print(f"[CALIB] Attempt {attempts}: avg_error={avg_error:.4f}, valid_points={valid_points}")

                if avg_error < CALIB_ERROR_THRESH:
                    break  # good enough, stop retrying

            except TimeoutError as exc:
                print(f"[GP] {exc}")
                await ws.send(json.dumps({
                    "type":   "calibration_failed",
                    "reason": "timeout",
                    "detail": str(exc),
                }))
                break

        return avg_error, valid_points, attempts
    
    def show_experimenter_alert():
        root = tk.Tk()
        root.withdraw()  # hide the tiny root window
        messagebox.showwarning(
            "Calibration Failed",
            "La calibration échoue trop souvent.\nAppellez l'expérimentateur."
        )
        root.destroy()

    async def receive_commands() -> None:
        try:
            async for raw in ws:
                msg = json.loads(raw)
                cmd = msg.get("cmd")

                if cmd == "trigger":
                    value = str(msg.get("value", "")).replace('"', "'")
                    tracker.user_data(value)
                    print(f"  [TRIGGER] {value}")
                elif cmd == "save_data":
                    data_type = msg.get("data_type", "unknown")
                    content = msg.get("content", "")
                    filename = os.path.join(FOLDER, f"{data_type}_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.{'csv' if 'csv' in data_type else 'json'}")
                    with open(filename, "w", encoding="utf-8") as f:
                        f.write(content)
                    print(f"[DATA] Saved {data_type} to {filename}")
                elif cmd == "calibrate":

                    avg_error, valid_points, attempts = await run_logic()

                    if attempts >= MAX_CALIB_ATTEMPTS:
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(None, show_experimenter_alert)
                        avg_error, valid_points, attempts = await run_logic()

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