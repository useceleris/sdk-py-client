import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# The objects the snippets assume already exist, as a reader would. The SDK is
# bound only under a private alias, so a snippet must import every name it
# uses.
PREAMBLE = """
import useceleris_client as _celeris

client: _celeris.Client
channel: _celeris.Channel
chat: _celeris.Segment
data: bytes
page: _celeris.PresencePage
metadata: _celeris.MessageMetadata


async def fetch_credentials(request: _celeris.CredentialRequest) -> dict[str, str]:
    raise NotImplementedError
"""


def snippets(document: str) -> list[str]:
    text = (ROOT / document).read_text()
    return re.findall(r"```python\n(.*?)```", text, flags=re.DOTALL)


# end function snippets


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


# end function test_documented_snippets_typecheck_against_the_public_surface
