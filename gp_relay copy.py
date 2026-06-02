"""
GazePoint WebSocket Relay (v2)
==============================
Requires: pip install websockets

1. Start Gazepoint Control
2. python gp_relay.py
3. Open experiment in browser
"""

import os
import time
import asyncio
import websockets
import socket
import json
import threading
import queue
import re
import xml.etree.ElementTree as ET
from opengaze import OpenGazeTracker

GP_HOST = "127.0.0.1"
GP_PORT = 4242
WS_HOST = "localhost"
WS_PORT = 8765

tracker = OpenGazeTracker(ip=GP_HOST, port=GP_PORT, logfile='gaze_data.csv') 
# TODO: change the logfile to capture name and time of the session 
time.sleep(1.0)


dirname = os.path.dirname(os.path.abspath(__file__))
fname = os.path.join(dirname, '%s.tsv' % (time.strftime("%Y-%m-%d_%H-%M-%S")))


# Enable the tracker to send ALL the things.
tracker.enable_send_counter(True)
tracker.enable_send_cursor(True)
tracker.enable_send_eye_left(True)
tracker.enable_send_eye_right(True)
tracker.enable_send_pog_best(True)
tracker.enable_send_pog_fix(True)
tracker.enable_send_pog_left(True)
tracker.enable_send_pog_right(True)
tracker.enable_send_pupil_left(True)
tracker.enable_send_pupil_right(True)
tracker.enable_send_time(True)
tracker.enable_send_time_tick(True)
tracker.enable_send_user_data(True)


# # # # #
# CALIBRATION

# Reset the calibration to its default points.
tracker.calibrate_reset()

# Show the calibration screen.
tracker.calibrate_show(True)

# Start the calibration.
tracker.calibrate_start(True)

# Wait for the calibration result.
result = None
while result == None:
	result = tracker.get_calibration_result()
	time.sleep(0.1)

# Hide the calibration window.
tracker.calibrate_show(False)


# # # # #
# DATA COLLECTION

# Start the streaming of data.
tracker.enable_send_data(True)

# Log the start.
tracker.user_data("START=%d" % (round(time.time()*1000)))

# Wait for 1 sample's duration, then reset the user-defined variable.
time.sleep(0.017)
tracker.user_data("0")

# Collect data for five seconds.
time.sleep(5.0)

# Log the end of data collection.
tracker.user_data("STOP=%d" % (round(time.time()*1000)))

# Stop the streaming of data.
tracker.enable_send_data(False)


# # # # #
# CLOSE

# Close the connection.
tracker.close()