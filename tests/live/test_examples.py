import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.live

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"


def test_runs_the_quickstart() -> None:
    result = subprocess.run(
        [sys.executable, str(EXAMPLES / "quickstart.py")],
        capture_output=True,
        text=True,
        timeout=60,
        env=dict(os.environ),
        check=True,
    )

    assert re.search(r"example: ok delivered=[1-9]\d* present=\d+", result.stdout), (
        result.stdout
    )


# end function test_runs_the_quickstart
