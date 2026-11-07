#!/usr/bin/env python3
"""Run a JavaScript snippet in Chrome's active tab and print the result (needs View > Developer > Allow JavaScript from Apple Events)."""
import os
import subprocess
import sys
import tempfile

with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
    f.write(sys.argv[1])
    path = f.name
try:
    out = subprocess.run(["osascript", "-e", f'set js to read POSIX file "{path}" as «class utf8»',
                          "-e", 'tell application "Google Chrome" to execute front window\'s active tab javascript js'],
                         capture_output=True, text=True, timeout=10)
    print(out.stdout.strip())
    sys.exit(0 if out.returncode == 0 else 1)
finally:
    os.unlink(path)
