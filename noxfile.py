"""The check: `nox` runs lint, typecheck, tests and package on every supported
Python. `nox -s live` runs the suites against the realtime service in .env."""

import ast
import shutil
import tarfile
import zipfile
from email.parser import Parser
from pathlib import Path

import nox
from packaging.requirements import Requirement

nox.options.default_venv_backend = "uv"
nox.options.reuse_venv = "yes"
nox.options.sessions = ["lint", "typecheck", "tests", "package"]

PYTHONS = ["3.10", "3.11", "3.12", "3.13", "3.14"]

ROOT = Path(__file__).parent

PACKAGE = "useceleris_client"

# AUTH-05: a client install carries no path to a signing facility, in its own
# code or in what it installs.
SIGNING_MODULES = {"hmac", "hashlib", "useceleris_server"}

RUNTIME_DEPENDENCIES = {"pydantic", "typing-extensions", "websockets"}

DEVELOPMENT = nox.project.dependency_groups(
    nox.project.load_toml("pyproject.toml"), "dev"
)


def requirement(name: str) -> str:
    """The pinned development requirement for one tool."""
    return next(entry for entry in DEVELOPMENT if entry.split("==")[0] == name)


@nox.session(python="3.14")
def lint(session: nox.Session) -> None:
    session.install(requirement("ruff"))
    session.run("ruff", "check", ".")
    session.run("ruff", "format", "--check", ".")


@nox.session(python="3.14")
def typecheck(session: nox.Session) -> None:
    session.install("-e", ".", *DEVELOPMENT)
    session.run("mypy")


@nox.session(python=PYTHONS)
def tests(session: nox.Session) -> None:
    session.install("-e", ".", *DEVELOPMENT)
    session.run("pytest", *session.posargs)


@nox.session(python=PYTHONS)
def live(session: nox.Session) -> None:
    session.install("-e", ".", *DEVELOPMENT)
    session.run("pytest", "-m", "live", "tests/live", *session.posargs)


@nox.session(python=PYTHONS)
def package(session: nox.Session) -> None:
    """Builds the sdist and wheel, checks what they contain, installs the wheel
    alone and uses it as a consumer would."""
    session.install(requirement("mypy"))
    work = Path(session.create_tmp())
    distribution = work / "dist"
    shutil.rmtree(distribution, ignore_errors=True)
    session.run("uv", "build", "--out-dir", str(distribution), str(ROOT), external=True)

    (wheel,) = distribution.glob("*.whl")
    (sdist,) = distribution.glob("*.tar.gz")
    check_wheel(wheel)
    check_sdist(sdist)

    session.install("--force-reinstall", str(wheel))
    consumer = work / "consumer"
    shutil.rmtree(consumer, ignore_errors=True)
    consumer.mkdir()
    shutil.copy(ROOT / "examples" / "quickstart.py", consumer)

    with session.chdir(consumer):
        session.run(
            "python",
            "-c",
            f"import {PACKAGE}, pathlib; "
            f"assert 'site-packages' in {PACKAGE}.__file__, {PACKAGE}.__file__; "
            f"assert (pathlib.Path({PACKAGE}.__file__).parent / 'py.typed').exists()",
        )
        session.run(
            "python",
            "-c",
            "import importlib.util; "
            "assert importlib.util.find_spec('useceleris_server') is None, "
            "'the client install brought in the signing package'",
        )
        # The shipped type information, checked from outside the repository.
        session.run("mypy", "--strict", "quickstart.py")


def check_wheel(wheel: Path) -> None:
    sources = sorted(
        path.relative_to(ROOT / "src").as_posix()
        for path in (ROOT / "src" / PACKAGE).rglob("*")
        if path.is_file() and "__pycache__" not in path.parts
    )

    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        packaged = sorted(name for name in names if name.startswith(f"{PACKAGE}/"))
        others = [name for name in names if not name.startswith(f"{PACKAGE}/")]
        (metadata_file,) = (name for name in names if name.endswith("/METADATA"))
        metadata = Parser().parsestr(archive.read(metadata_file).decode())

        assert packaged == sources, f"Wheel files differ from src: {packaged}"
        assert all(".dist-info/" in name for name in others), others

        for name in packaged:
            if name.endswith(".py"):
                assert_no_signing(name, archive.read(name).decode())

    required = {
        Requirement(entry).name for entry in metadata.get_all("Requires-Dist") or []
    }
    assert required == RUNTIME_DEPENDENCIES, required


def assert_no_signing(name: str, source: str) -> None:
    imported: set[str] = set()

    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])

    assert not imported & SIGNING_MODULES, (
        f"{name} imports {imported & SIGNING_MODULES}"
    )
    assert "signing_secret" not in source, f"{name} mentions a signing secret"


def check_sdist(sdist: Path) -> None:
    with tarfile.open(sdist) as archive:
        names = [name.split("/", 1)[1] for name in archive.getnames() if "/" in name]

    top_level = {name.split("/")[0] for name in names}
    assert top_level == {
        "src",
        "README.md",
        "LICENSE",
        "pyproject.toml",
        "PKG-INFO",
        # Hatch includes the ignore file it used to select the contents.
        ".gitignore",
    }, top_level
    assert not any("__pycache__" in name or name.endswith(".env") for name in names)
