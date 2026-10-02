import re
import subprocess
import sys
from pathlib import Path

import useceleris_client

ROOT = Path(__file__).resolve().parents[1]

# The names the snippets assume are already in scope, as a reader would.
PREAMBLE = f"""
from useceleris_client import {", ".join(useceleris_client.__all__)}

client: Client
channel: Channel
chat: Segment
data: bytes
page: PresencePage
metadata: MessageMetadata


async def fetch_credentials(request: CredentialRequest) -> dict[str, str]:
    raise NotImplementedError
"""


def snippets(document: str) -> list[str]:
    text = (ROOT / document).read_text()
    return re.findall(r"```python\n(.*?)```", text, flags=re.DOTALL)


def test_documented_snippets_typecheck_against_the_public_surface(
    tmp_path: Path,
) -> None:
    # Each snippet becomes the body of its own async function, so it may await
    # and its names stay its own.
    blocks = snippets("README.md") + snippets("EXAMPLES.md")
    assert len(blocks) >= 10

    module = PREAMBLE + "".join(
        f"\n\nasync def snippet_{index}() -> None:\n"
        + "".join(f"    {line}\n" if line else "\n" for line in block.splitlines())
        for index, block in enumerate(blocks)
    )
    path = tmp_path / "snippets.py"
    path.write_text(module)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            "--strict",
            "--no-incremental",
            "--python-version",
            "3.10",
            str(path),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
