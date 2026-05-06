"""pytest config for browser-visit-tools.

Adds the parent directory to sys.path so `import reading_list` works
from any test file.  Mirrors the pattern used by browser-visit-logger,
but stays self-contained — no cross-project imports.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
