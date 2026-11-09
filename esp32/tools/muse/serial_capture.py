#!/usr/bin/env python3
"""Capture serial output from the gadget for a fixed duration, then reset the
line. Usage: python serial_capture.py COM5 12
The optional second arg is seconds (default 10)."""
import sys
import time

import serial

port = sys.argv[1] if len(sys.argv) > 1 else "COM5"
seconds = float(sys.argv[2]) if len(sys.argv) > 2 else 10.0

ser = serial.Serial(port, 115200, timeout=0.2)
# Pass "--reset" as a third arg to pulse RTS and reboot the board; otherwise just
# listen so a push-to-talk press isn't missed by an unexpected reset.
if len(sys.argv) > 3 and sys.argv[3] == "--reset":
    ser.setDTR(False)
    ser.setRTS(True)
    time.sleep(0.1)
    ser.setRTS(False)
    time.sleep(0.1)

deadline = time.time() + seconds
while time.time() < deadline:
    data = ser.read(4096)
    if data:
        try:
            sys.stdout.write(data.decode("utf-8", "replace"))
        except Exception:
            sys.stdout.write(repr(data))
        sys.stdout.flush()
ser.close()
