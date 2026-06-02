# PyOpenGaze: Python wrapper for the OpenGaze API.
#
# author: Edwin Dalmaijer
# email: edwin.dalmaijer@psy.ox.ac.uk
#
# Version 1 (27-Apr-2016)
# Modernised for Python 3.11+ (2024)

import os
import copy
import time
import socket
import datetime
import lxml.etree
from queue import Queue
from threading import Event, Lock, Thread


class OpenGazeTracker:

    def __init__(
        self,
        ip: str = "127.0.0.1",
        port: int = 4242,
        logfile: str = "default.tsv",
        debug: bool = False,
    ) -> None:
        """The OpenGazeConnection class communicates to the GazePoint
        server through a TCP/IP socket. Incoming samples will be written
        to a log at the specified path.

        Keyword Arguments

        ip      -   The IP address of the computer that is running the
                    OpenGaze server. Usually the localhost at 127.0.0.1.
                    Type: str. Default = '127.0.0.1'

        port    -   The port number that the OpenGaze server is on; usually
                    4242. Type: int. Default = 4242

        logfile -   The path to the intended log file, including a
                    file extension ('.tsv'). Type: str. Default = 'default.tsv'

        debug   -   Boolean that determines whether DEBUG mode should be
                    active (True) or not (False). In DEBUG mode, all sent
                    and received messages are logged to a file.
                    Type: bool. Default = False
        """

        # DEBUG
        self._debug = debug
        if self._debug:
            dt = time.strftime("%Y-%m-%d_%H-%M-%S")
            self._debuglog = open(f"debug_{dt}.txt", "w")
            self._debuglog.write(f"OPENGAZE PYTHON DEBUG LOG {dt}\n")
            self._debugcounter = 0
            self._debugconsolidatefreq = 100

        # CONNECTION
        self.host = ip
        self.port = port
        self._debug_print(f"Connecting to {self.host} ({self.port})...")
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((self.host, self.port))
        self._sock.settimeout(1.0)
        self._debug_print("Successfully connected!")
        self._maxrecvsize = 4096
        self._socklock = Lock()
        self._connected = Event()
        self._connected.set()
        self._current_calibration_point: int | None = None

        # LOGGING
        self._debug_print(f"Opening new logfile '{logfile}'")
        self._logfile = open(logfile, "w")
        self._logheader = [
            "CNT", "TIME", "TIME_TICK",
            "FPOGX", "FPOGY", "FPOGS", "FPOGD", "FPOGID", "FPOGV",
            "LPOGX", "LPOGY", "LPOGV",
            "RPOGX", "RPOGY", "RPOGV",
            "BPOGX", "BPOGY", "BPOGV",
            "LPCX", "LPCY", "LPD", "LPS", "LPV",
            "RPCX", "RPCY", "RPD", "RPS", "RPV",
            "LEYEX", "LEYEY", "LEYEZ", "LPUPILD", "LPUPILV",
            "REYEX", "REYEY", "REYEZ", "RPUPILD", "RPUPILV",
            "CX", "CY", "CS",
            "USER",
        ]
        self._n_logvars = len(self._logheader)
        self._logfile.write("\t".join(self._logheader) + "\n")
        self._logcounter = 0
        self._log_consolidation_freq = 60
        self._logqueue: Queue = Queue()
        self._logging = Event()
        self._logging.set()
        self._log_ready_for_closing = Event()
        self._log_ready_for_closing.clear()
        self._logthread = Thread(
            target=self._process_logging,
            name="PyGaze_OpenGazeConnection_logging",
            daemon=True,
        )

        # INCOMING
        self._incoming: dict = {}
        self._acknowledgements: dict = {}
        self._inlock = Lock()
        self._acklock = Lock()
        self._unfinished = ""
        self._inthread = Thread(
            target=self._process_incoming,
            name="PyGaze_OpenGazeConnection_incoming",
            daemon=True,
        )

        # OUTGOING
        self._outqueue: Queue = Queue()
        self._sock_ready_for_closing = Event()
        self._sock_ready_for_closing.clear()
        self._outthread = Thread(
            target=self._process_outgoing,
            name="PyGaze_OpenGazeConnection_outgoing",
            daemon=True,
        )
        self._outlatest: dict = {}
        self._outlock = Lock()

        # SHUTDOWN SIGNAL
        self._thread_shutdown_signal = "KILL_ALL_HUMANS"

        # START THREADS
        self._debug_print("Starting the logging thread.")
        self._logthread.start()
        self._debug_print("Starting the incoming thread.")
        self._inthread.start()
        self._debug_print("Starting the outgoing thread.")
        self._outthread.start()

        # ENABLE ALL DATA FIELDS
        time.sleep(0.5)
        self.enable_send_counter(True)
        self.enable_send_cursor(True)
        self.enable_send_eye_left(True)
        self.enable_send_eye_right(True)
        self.enable_send_pog_best(True)
        self.enable_send_pog_fix(True)
        self.enable_send_pog_left(True)
        self.enable_send_pog_right(True)
        self.enable_send_pupil_left(True)
        self.enable_send_pupil_right(True)
        self.enable_send_time(True)
        self.enable_send_time_tick(True)
        self.enable_send_user_data(True)
        self.user_data("0")

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    def calibrate(self) -> list | None:
        """Calibrates the eye tracker."""
        self.clear_calibration_result()
        self.calibrate_show(True)
        self.calibrate_start(True)
        result = None
        while result is None:
            result = self.get_calibration_result()
            time.sleep(0.1)
        self.calibrate_show(False)
        return result

    def sample(self) -> tuple[float | None, float | None]:
        """Return the current best point-of-gaze as (x, y)."""
        with self._inlock:
            try:
                rec = self._incoming["REC"]["NO_ID"]
                x = float(rec["BPOGX"])
                y = float(rec["BPOGY"])
            except KeyError:
                x, y = None, None
        return x, y

    def pupil_size(self) -> float | None:
        """Return the current average pupil size (or None if invalid)."""
        with self._inlock:
            try:
                rec = self._incoming["REC"]["NO_ID"]
                _ = rec["LPV"], rec["LPS"], rec["RPV"], rec["RPS"]  # key check
            except KeyError:
                return None

            n = 0
            psize = 0.0
            if str(rec["LPV"]) == "1":
                psize += float(rec["LPS"])
                n += 1
            if str(rec["RPV"]) == "1":
                psize += float(rec["RPS"])
                n += 1

        return None if n == 0 else psize / n

    def log(self, message: str) -> None:
        """Log a message to the log file. ONLY CALL WHILE RECORDING DATA."""
        i = copy.copy(self._logcounter)
        self.user_data(message)
        while self._logcounter <= i:
            time.sleep(0.0001)
        self.user_data("0")

    def start_recording(self) -> None:
        """Start writing data to the log file."""
        self.enable_send_data(True)

    def stop_recording(self) -> None:
        """Pause writing data to the log file."""
        self.enable_send_data(False)

    def close(self) -> None:
        """Close the tracker connection, log files, and all threads."""
        self.user_data("0")

        self._debug_print("Unsetting the connection event")
        self._connected.clear()

        self._debug_print("Adding stop signal to outgoing Queue")
        self._outqueue.put(self._thread_shutdown_signal)
        self._debug_print("Adding stop signal to logging Queue")
        self._logqueue.put(self._thread_shutdown_signal)

        self._debug_print("Waiting for the socket to close...")
        self._sock_ready_for_closing.wait()

        self._debug_print("Closing socket connection...")
        self._sock.close()
        self._debug_print("Socket connection closed!")

        self._debug_print("Waiting for the log to close...")
        self._log_ready_for_closing.wait()

        self._logfile.close()
        self._debug_print("Log closed!")

        self._debug_print("Waiting for the Threads to join...")
        self._outthread.join()
        self._debug_print("Outgoing Thread joined!")
        self._inthread.join()
        self._debug_print("Incoming Thread joined!")
        self._logthread.join()
        self._debug_print("Logging Thread joined!")

        if self._debug:
            self._debuglog.write("END OF DEBUG LOG")
            self._debuglog.close()

    # -----------------------------------------------------------------------
    # enable_send_* methods
    # -----------------------------------------------------------------------

    def enable_send_data(self, state: bool) -> bool:
        """Start (True) or stop (False) streaming of data from the server."""
        ack, timeout = self._send_message(
            "SET", "ENABLE_SEND_DATA", values=[("STATE", int(state))]
        )
        return ack and not timeout

    def enable_send_counter(self, state: bool) -> bool:
        ack, timeout = self._send_message(
            "SET", "ENABLE_SEND_COUNTER", values=[("STATE", int(state))]
        )
        return ack and not timeout

    def enable_send_time(self, state: bool) -> bool:
        ack, timeout = self._send_message(
            "SET", "ENABLE_SEND_TIME", values=[("STATE", int(state))]
        )
        return ack and not timeout

    def enable_send_time_tick(self, state: bool) -> bool:
        ack, timeout = self._send_message(
            "SET", "ENABLE_SEND_TIME_TICK", values=[("STATE", int(state))]
        )
        return ack and not timeout

    def enable_send_pog_fix(self, state: bool) -> bool:
        ack, timeout = self._send_message(
            "SET", "ENABLE_SEND_POG_FIX", values=[("STATE", int(state))]
        )
        return ack and not timeout

    def enable_send_pog_left(self, state: bool) -> bool:
        ack, timeout = self._send_message(
            "SET", "ENABLE_SEND_POG_LEFT", values=[("STATE", int(state))]
        )
        return ack and not timeout

    def enable_send_pog_right(self, state: bool) -> bool:
        ack, timeout = self._send_message(
            "SET", "ENABLE_SEND_POG_RIGHT", values=[("STATE", int(state))]
        )
        return ack and not timeout

    def enable_send_pog_best(self, state: bool) -> bool:
        ack, timeout = self._send_message(
            "SET", "ENABLE_SEND_POG_BEST", values=[("STATE", int(state))]
        )
        return ack and not timeout

    def enable_send_pupil_left(self, state: bool) -> bool:
        """Enable/disable left pupil data (LPCX, LPCY, LPD, LPS, LPV)."""
        ack, timeout = self._send_message(
            "SET", "ENABLE_SEND_PUPIL_LEFT", values=[("STATE", int(state))]
        )
        return ack and not timeout

    def enable_send_pupil_right(self, state: bool) -> bool:
        """Enable/disable right pupil data (RPCX, RPCY, RPD, RPS, RPV)."""
        ack, timeout = self._send_message(
            "SET", "ENABLE_SEND_PUPIL_RIGHT", values=[("STATE", int(state))]
        )
        return ack and not timeout

    def enable_send_eye_left(self, state: bool) -> bool:
        """Enable/disable 3D left eye data (LEYEX/Y/Z, LPUPILD, LPUPILV)."""
        ack, timeout = self._send_message(
            "SET", "ENABLE_SEND_EYE_LEFT", values=[("STATE", int(state))]
        )
        return ack and not timeout

    def enable_send_eye_right(self, state: bool) -> bool:
        """Enable/disable 3D right eye data (REYEX/Y/Z, RPUPILD, RPUPILV)."""
        ack, timeout = self._send_message(
            "SET", "ENABLE_SEND_EYE_RIGHT", values=[("STATE", int(state))]
        )
        return ack and not timeout

    def enable_send_cursor(self, state: bool) -> bool:
        """Enable/disable mouse cursor data (CX, CY, CS)."""
        ack, timeout = self._send_message(
            "SET", "ENABLE_SEND_CURSOR", values=[("STATE", int(state))]
        )
        return ack and not timeout

    def enable_send_user_data(self, state: bool) -> bool:
        """Enable/disable user-defined variable in the data record."""
        ack, timeout = self._send_message(
            "SET", "ENABLE_SEND_USER_DATA", values=[("STATE", int(state))]
        )
        return ack and not timeout

    # -----------------------------------------------------------------------
    # Calibration methods
    # -----------------------------------------------------------------------

    def calibrate_start(self, state: bool) -> bool:
        """Start (True) or stop (False) the calibration procedure."""
        self._current_calibration_point = 0 if state else None
        ack, timeout = self._send_message(
            "SET", "CALIBRATE_START", values=[("STATE", int(state))]
        )
        return ack and not timeout

    def calibrate_show(self, state: bool) -> bool:
        """Show (True) or hide (False) the calibration window."""
        ack, timeout = self._send_message(
            "SET", "CALIBRATE_SHOW", values=[("STATE", int(state))]
        )
        return ack and not timeout

    def calibrate_timeout(self, value: float) -> bool:
        """Set calibration point duration in seconds."""
        ack, timeout = self._send_message(
            "SET", "CALIBRATE_TIMEOUT", values=[("VALUE", float(value))]
        )
        return ack and not timeout

    def calibrate_delay(self, value: float) -> bool:
        """Set calibration animation duration in seconds."""
        ack, timeout = self._send_message(
            "SET", "CALIBRATE_DELAY", values=[("VALUE", float(value))]
        )
        return ack and not timeout

    def calibrate_result_summary(self) -> tuple[str | None, str | None]:
        """Return (AVE_ERROR, VALID_POINTS) from the calibration summary."""
        ack, timeout = self._send_message(
            "GET", "CALIBRATE_RESULT_SUMMARY", values=None
        )
        if not ack:
            return None, None
        with self._inlock:
            cal = self._incoming["ACK"]["CALIBRATE_RESULT_SUMMARY"]
            return copy.copy(cal["AVE_ERROR"]), copy.copy(cal["VALID_POINTS"])

    def calibrate_clear(self) -> bool:
        """Clear the internal list of calibration points."""
        ack, timeout = self._send_message("SET", "CALIBRATE_CLEAR", values=None)
        return ack and not timeout

    def calibrate_reset(self) -> bool:
        """Reset the internal list of calibration points to defaults."""
        ack, timeout = self._send_message("SET", "CALIBRATE_RESET", values=None)
        return ack and not timeout

    def calibrate_addpoint(self, x: float, y: float) -> bool:
        """Add a calibration point at normalised (x, y) coordinates."""
        ack, timeout = self._send_message(
            "SET", "CALIBRATE_ADDPOINT", values=[("X", x), ("Y", y)]
        )
        return ack and not timeout

    def get_calibration_points(self) -> list[tuple[float, float]] | None:
        """Return the current list of calibration points."""
        ack, timeout = self._send_message(
            "GET", "CALIBRATE_ADDPOINT", values=None
        )
        if not ack:
            return None
        points = []
        with self._inlock:
            cal = self._incoming["ACK"]["CALIBRATE_ADDPOINT"]
            n = int(cal["PTS"])
            for i in range(1, n + 1):
                points.append((
                    float(cal[f"X{i}"]),
                    float(cal[f"Y{i}"]),
                ))
        return points

    def clear_calibration_result(self) -> None:
        """Clear the internally stored calibration result."""
        with self._inlock:
            if "CAL" in self._incoming:
                self._incoming["CAL"].pop("CALIB_RESULT", None)

    def get_calibration_result(self) -> list[dict] | None:
        """Return the latest calibration results as a list of dicts, or None."""
        params = ["CALX", "CALY", "LX", "LY", "LV", "RX", "RY", "RV"]
        with self._inlock:
            try:
                cal = copy.deepcopy(self._incoming["CAL"]["CALIB_RESULT"])
            except KeyError:
                return None

        n_points = (len(cal) - 1) // len(params)
        points = []
        for i in range(1, n_points + 1):
            p: dict = {}
            for par in params:
                key = f"{par}{i}"
                if par in ("LV", "RV"):
                    p[par] = cal[key] == "1"
                else:
                    p[par] = float(cal[key])
            points.append(p)
        return points

    def wait_for_calibration_point_start(
        self, timeout: float = 10.0
    ) -> tuple[int | None, tuple[float, float] | None]:
        """Wait for the next calibration point to start.

        Returns (point_number, (x, y)) or (None, None) on timeout.
        """
        start = time.monotonic()

        # Wait for CALIBRATE_START acknowledgement
        t0: float | None = None
        while t0 is None and time.monotonic() - start < timeout:
            with self._inlock:
                try:
                    t0 = self._incoming["ACK"]["CALIBRATE_START"]["t"]
                except KeyError:
                    pass
            if t0 is None:
                time.sleep(0.001)

        if t0 is None:
            return None, None

        # Wait for a new calibration point
        while time.monotonic() - start < timeout:
            t1 = 0.0
            pt_nr = None
            x = y = 0.0

            with self._inlock:
                try:
                    pt = self._incoming["CAL"]["CALIB_START_PT"]
                    t1 = pt["t"]
                    pt_nr = int(pt["PT"])
                    x = float(pt["CALX"])
                    y = float(pt["CALY"])
                except KeyError:
                    pass

            if t1 >= t0 and pt_nr is not None and pt_nr != self._current_calibration_point:
                self._current_calibration_point = pt_nr
                return pt_nr, (x, y)

            time.sleep(0.001)

        return None, None

    # -----------------------------------------------------------------------
    # Device info / settings
    # -----------------------------------------------------------------------

    def user_data(self, value: str) -> bool:
        """Set the user-data field to embed custom markers in the stream."""
        ack, timeout = self._send_message(
            "SET", "USER_DATA", values=[("VALUE", str(value))]
        )
        return ack and not timeout

    def tracker_display(self, state: bool) -> bool:
        """Show (True) or hide (False) the eye-tracker display window."""
        ack, timeout = self._send_message(
            "SET", "TRACKER_DISPLAY", values=[("STATE", int(state))]
        )
        return ack and not timeout

    def time_tick_frequency(self) -> str | None:
        """Return the time-tick frequency (alias for get_time_tick_frequency)."""
        return self.get_time_tick_frequency()

    def get_time_tick_frequency(self) -> str | None:
        """Return the TIME_TICK frequency needed to convert ticks to seconds."""
        ack, timeout = self._send_message("GET", "TIME_TICK_FREQUENCY", values=None)
        if not ack:
            return None
        with self._inlock:
            return copy.copy(self._incoming["ACK"]["TIME_TICK_FREQUENCY"]["FREQ"])

    def screen_size(self, x: int, y: int, w: int, h: int) -> bool:
        """Set the gaze-tracking screen position (x, y) and size (w, h) in px."""
        ack, timeout = self._send_message(
            "SET", "SCREEN_SIZE",
            values=[("X", x), ("Y", y), ("WIDTH", w), ("HEIGHT", h)],
        )
        return ack and not timeout

    def get_screen_size(self) -> list[str | None]:
        """Return [x, y, w, h] of the tracking screen in pixels."""
        ack, timeout = self._send_message("GET", "SCREEN_SIZE", values=None)
        if not ack:
            return [None, None, None, None]
        with self._inlock:
            s = self._incoming["ACK"]["SCREEN_SIZE"]
            return [copy.copy(s["X"]), copy.copy(s["Y"]),
                    copy.copy(s["WIDTH"]), copy.copy(s["HEIGHT"])]

    def camera_size(self) -> list[str | None]:
        """Return [w, h] of the camera sensor in pixels (alias)."""
        return self.get_camera_size()

    def get_camera_size(self) -> list[str | None]:
        """Return [w, h] of the camera sensor in pixels."""
        ack, timeout = self._send_message("GET", "CAMERA_SIZE", values=None)
        if not ack:
            return [None, None]
        with self._inlock:
            s = self._incoming["ACK"]["CAMERA_SIZE"]
            return [copy.copy(s["WIDTH"]), copy.copy(s["HEIGHT"])]

    def product_id(self) -> str | None:
        """Return the eye-tracker product identifier (alias)."""
        return self.get_product_id()

    def get_product_id(self) -> str | None:
        ack, timeout = self._send_message("GET", "PRODUCT_ID", values=None)
        if not ack:
            return None
        with self._inlock:
            return copy.copy(self._incoming["ACK"]["PRODUCT_ID"]["VALUE"])

    def serial_id(self) -> str | None:
        """Return the eye-tracker serial number (alias)."""
        return self.get_serial_id()

    def get_serial_id(self) -> str | None:
        ack, timeout = self._send_message("GET", "SERIAL_ID", values=None)
        if not ack:
            return None
        with self._inlock:
            return copy.copy(self._incoming["ACK"]["SERIAL_ID"]["VALUE"])

    def company_id(self) -> str | None:
        """Return the manufacturer identifier (alias)."""
        return self.get_company_id()

    def get_company_id(self) -> str | None:
        ack, timeout = self._send_message("GET", "COMPANY_ID", values=None)
        if not ack:
            return None
        with self._inlock:
            return copy.copy(self._incoming["ACK"]["COMPANY_ID"]["VALUE"])

    def api_id(self) -> str | None:
        """Return the API version number (alias)."""
        return self.get_api_id()

    def get_api_id(self) -> str | None:
        ack, timeout = self._send_message("GET", "API_ID", values=None)
        if not ack:
            return None
        with self._inlock:
            return copy.copy(self._incoming["ACK"]["API_ID"]["VALUE"])

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------

    def _debug_print(self, msg: str) -> None:
        if not self._debug:
            return
        self._debuglog.write(
            f"{datetime.datetime.now().strftime('%H:%M:%S.%f')}: {msg}\n"
        )
        if self._debugcounter % self._debugconsolidatefreq == 0:
            self._debuglog.flush()
            os.fsync(self._debuglog.fileno())
        self._debugcounter += 1

    def _format_msg(
        self,
        command: str,
        ID: str,
        values: list[tuple[str, object]] | None = None,
    ) -> str:
        xml = f'<{command.upper()} ID="{ID.upper()}" '
        if values:
            for par, val in values:
                xml += f'{par.upper()}="{val}" '
        xml += "/>\r\n"
        return xml

    def _log_consolidation(self) -> None:
        self._logfile.flush()
        os.fsync(self._logfile.fileno())

    def _log_sample(self, sample: dict) -> None:
        line = [""] * self._n_logvars
        for varname, value in sample.items():
            if varname in self._logheader:
                line[self._logheader.index(varname)] = value
        self._logfile.write("\t".join(line) + "\n")

    def _parse_msg(self, xml: str) -> tuple[str, dict]:
        e = lxml.etree.fromstring(xml)
        return e.tag, dict(e.attrib)

    # -----------------------------------------------------------------------
    # Threads
    # -----------------------------------------------------------------------

    def _process_logging(self) -> None:
        self._debug_print("Logging Thread started.")
        while not self._log_ready_for_closing.is_set():
            sample = self._logqueue.get()
            if sample == self._thread_shutdown_signal:
                self._log_ready_for_closing.set()
                break
            self._log_sample(sample)
            if self._logcounter % self._log_consolidation_freq == 0:
                self._log_consolidation()
            self._logcounter += 1
        self._debug_print("Logging Thread ended.")

    def _process_incoming(self) -> None:
        self._debug_print("Incoming Thread started.")
        while self._connected.is_set():
            self._socklock.acquire()
            timeout = False
            try:
                raw = self._sock.recv(self._maxrecvsize)
            except socket.timeout:
                timeout = True
            t = time.time()
            self._socklock.release()

            if timeout:
                self._debug_print("socket recv timeout")
                continue

            self._debug_print(f"Raw instring: {raw!r}")
            instring = raw.decode("utf-8", errors="replace")
            messages = instring.split("\r\n")

            if self._unfinished:
                messages[0] = self._unfinished + messages[0]
                self._unfinished = ""

            if messages and not messages[-1].endswith("/>"):
                self._unfinished = messages.pop()

            for msg in messages:
                if not msg.strip():
                    continue
                self._debug_print(f"Incoming: {msg!r}")
                try:
                    command, msgdict = self._parse_msg(msg)
                except Exception:
                    continue

                if command == "ACK":
                    with self._acklock:
                        self._acknowledgements[msgdict["ID"]] = t

                with self._inlock:
                    self._incoming.setdefault(command, {})
                    msgdict.setdefault("ID", "NO_ID")
                    msg_id = msgdict["ID"]
                    self._incoming[command].setdefault(msg_id, {})
                    self._incoming[command][msg_id]["t"] = t
                    for par, val in msgdict.items():
                        self._incoming[command][msg_id][par] = val

                    if command == "REC" and self._logging.is_set():
                        self._logqueue.put(
                            copy.deepcopy(self._incoming[command][msg_id])
                        )

        self._debug_print("Incoming Thread ended.")

    def _process_outgoing(self) -> None:
        self._debug_print("Outgoing Thread started.")
        while not self._sock_ready_for_closing.is_set():
            msg = self._outqueue.get()
            if msg == self._thread_shutdown_signal:
                self._sock_ready_for_closing.set()
                break

            self._debug_print(f"Outgoing: {msg!r}")
            with self._socklock:
                t = time.time()
                self._sock.send(msg.encode() if isinstance(msg, str) else msg)

            with self._outlock:
                self._outlatest[msg] = t

        self._debug_print("Outgoing Thread ended.")

    def _send_message(
        self,
        command: str,
        ID: str,
        values: list[tuple[str, object]] | None = None,
        wait_for_acknowledgement: bool = True,
        resend_timeout: float = 3.0,
        maxwait: float = 9.0,
    ) -> tuple[bool, bool]:
        msg = self._format_msg(command, ID, values=values)
        acknowledged = False
        timeout = False
        t0 = time.monotonic()

        while not acknowledged and not timeout:
            self._debug_print(f"Outqueue add: {msg!r}")
            self._outqueue.put(msg)

            if not wait_for_acknowledgement:
                break

            sent = False
            t1 = time.monotonic()
            while time.monotonic() - t1 < resend_timeout and not acknowledged:
                if not sent:
                    with self._outlock:
                        if msg in self._outlatest:
                            t_sent = self._outlatest[msg]
                            sent = True
                            self._debug_print(f"Outqueue sent: {msg!r}")
                    time.sleep(0.001)
                else:
                    with self._acklock:
                        if ID in self._acknowledgements and self._acknowledgements[ID] >= t_sent:
                            acknowledged = True
                            self._debug_print(f"Outqueue acknowledged: {msg!r}")
                    time.sleep(0.001)

                if not acknowledged and time.monotonic() - t0 > maxwait:
                    timeout = True
                    break

        return acknowledged, timeout