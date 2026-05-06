"""pytest config for browser-visit-tools.

Adds the parent directory to sys.path so `import reading_list` works
from any test file.  Mirrors the pattern used by browser-visit-logger,
but stays self-contained — no cross-project imports.

Also pins the local timezone to UTC for the test session so
``_format_timestamp`` (which converts stored UTC into local time)
produces deterministic output.  Tests that need to verify the local
conversion actually happens override TZ for their own scope.
"""
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

os.environ['TZ'] = 'UTC'
time.tzset()
