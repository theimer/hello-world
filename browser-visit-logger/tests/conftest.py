"""
pytest configuration for browser-visit-logger tests.

Sets BVL_HOST_LOG to a temp path before importing host so the RotatingFileHandler
(which runs at module-import time) never writes to ~/browser-visits-host.log during
the test run.  Then adds native-host/ to sys.path so `import host` resolves.
"""
import os
import sys
import tempfile
from pathlib import Path

# Redirect the host's internal log file away from ~ during tests.
# Must be set before `import host` anywhere in the test session.
_test_host_log = os.path.join(tempfile.gettempdir(), 'bvl-test-host.log')
os.environ.setdefault('BVL_HOST_LOG', _test_host_log)

# Make `import host` work from any test file.
sys.path.insert(0, str(Path(__file__).parent.parent / 'native-host'))
