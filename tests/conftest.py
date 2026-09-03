"""Test-runtime guarantees shared by local and CI Spark sessions."""

import os
import sys

# Spark otherwise falls back to a platform `python3`/`python` launcher, which
# can silently differ from the interpreter running pytest (especially on Windows).
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_PYTHON"] = sys.executable
