"""The house layout rules that neither ruff nor mypy can enforce, checked over
every Python file in the repository:

- A blank line follows every compound statement (``if``, ``for``, ``while``,
  ``with``, ``try``, ``match``, ``def``, ``class``) before the next statement
  in the same block.
- Every function, method and class ends with a marker comment that names it,
  ``# end function name``, ``# end method name`` or ``# end class Name``, as
  the first line after its body at the definition's own indentation. Only
  blank lines, which ``ruff format`` inserts, may come between.
"""

import ast
import io
import re
import tokenize
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

CHECKED = ["src", "tests", "examples", "noxfile.py"]

MARKER = re.compile(r"# end (function|method|class) ")

DEFINITIONS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)


def checked_files() -> list[Path]:
    files: list[Path] = []

    for entry in CHECKED:
        path = ROOT / entry

        if path.is_file():
            files.append(path)
        else:
            files.extend(sorted(path.rglob("*.py")))

    return files


# end function checked_files


@dataclass
class SourceFile:
    path: Path
    lines: list[str]
    # Line number -> (column, text) of each comment that fills its line.
    comment_lines: dict[int, tuple[int, str]]

    @classmethod
    def read(cls, path: Path) -> "SourceFile":
        source = path.read_text()
        comment_lines: dict[int, tuple[int, str]] = {}
        lines = source.splitlines()

        for token in tokenize.generate_tokens(io.StringIO(source).readline):
            line, column = token.start

            if token.type == tokenize.COMMENT and not lines[line - 1][:column].strip():
                comment_lines[line] = (column, token.string)

        return cls(path, lines, comment_lines)

    # end method read

    def is_blank(self, line: int) -> bool:
        return line <= len(self.lines) and not self.lines[line - 1].strip()

    # end method is_blank

    def skip_block_comments(self, line: int, column: int) -> int:
        """The last line of a block that ends on ``line``, past the comments
        indented into the block of a statement at ``column``."""
        while line + 1 in self.comment_lines:
            comment_column, _ = self.comment_lines[line + 1]

            if comment_column <= column:
                break

            line += 1

        return line

    # end method skip_block_comments


# end class SourceFile


class LayoutCheck:
    def __init__(self, source: SourceFile, display_name: str) -> None:
        self.source = source
        self.display_name = display_name
        self.violations: list[str] = []
        self.marker_lines: set[int] = set()

    # end method __init__

    def report(self, line: int, problem: str) -> None:
        self.violations.append(f"{self.display_name}:{line}: {problem}")

    # end method report

    def check_block(self, statements: list[ast.stmt], kind: str) -> int:
        """Checks one block's statements, and returns the line the block ends
        on. kind is how a definition directly inside it is named."""
        last_line = 0

        for index, statement in enumerate(statements):
            last_line = self.last_line(statement, kind)

            if index + 1 == len(statements) or not is_compound(statement):
                continue

            if not self.source.is_blank(last_line + 1):
                self.report(
                    last_line + 1,
                    f"leave a blank line after the block that starts on line "
                    f"{statement.lineno}",
                )

        return last_line

    # end method check_block

    def last_line(self, statement: ast.stmt, kind: str) -> int:
        """The line a statement ends on, its end marker included. Checks the
        blocks inside it on the way."""
        if isinstance(statement, DEFINITIONS):
            return self.check_definition(statement, kind)

        blocks = nested_blocks(statement)

        if not blocks:
            assert statement.end_lineno is not None
            return statement.end_lineno

        last_line = 0

        for block in blocks:
            last_line = self.check_block(block, "function")

        return self.source.skip_block_comments(last_line, statement.col_offset)

    # end method last_line

    def check_definition(
        self,
        definition: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
        kind: str,
    ) -> int:
        inner_kind = "method" if isinstance(definition, ast.ClassDef) else "function"
        body_end = self.source.skip_block_comments(
            self.check_block(definition.body, inner_kind), definition.col_offset
        )
        marker = f"# end {'class' if inner_kind == 'method' else kind} "
        marker += definition.name
        line = body_end + 1

        while self.source.is_blank(line):
            line += 1

        if self.source.comment_lines.get(line) != (definition.col_offset, marker):
            self.report(
                definition.lineno,
                f"end {definition.name} with {marker!r} on the line after its "
                f"body, at column {definition.col_offset}",
            )
            return body_end

        self.marker_lines.add(line)
        return line

    # end method check_definition

    def check_stray_markers(self) -> None:
        for line, (_, text) in self.source.comment_lines.items():
            if MARKER.match(text) and line not in self.marker_lines:
                self.report(line, "this end marker belongs to no definition")

    # end method check_stray_markers


# end class LayoutCheck


def is_compound(statement: ast.stmt) -> bool:
    return bool(nested_blocks(statement)) or isinstance(statement, DEFINITIONS)


# end function is_compound


def nested_blocks(statement: ast.stmt) -> list[list[ast.stmt]]:
    """The statement's blocks in source order, empty ones left out."""
    match statement:
        case ast.If() | ast.For() | ast.AsyncFor() | ast.While():
            blocks = [statement.body, statement.orelse]
        case ast.With() | ast.AsyncWith():
            blocks = [statement.body]
        case ast.Try():
            blocks = [
                statement.body,
                *(handler.body for handler in statement.handlers),
                statement.orelse,
                statement.finalbody,
            ]
        case ast.Match():
            blocks = [case.body for case in statement.cases]
        case _:
            blocks = []

    return [block for block in blocks if block]


# end function nested_blocks


def layout_violations(path: Path, display_name: str) -> list[str]:
    source = SourceFile.read(path)
    check = LayoutCheck(source, display_name)
    check.check_block(ast.parse("\n".join(source.lines)).body, "function")
    check.check_stray_markers()
    return check.violations


# end function layout_violations


def test_every_file_follows_the_layout_rules() -> None:
    violations = [
        violation
        for path in checked_files()
        for violation in layout_violations(path, str(path.relative_to(ROOT)))
    ]

    assert violations == [], "\n" + "\n".join(violations)


# end function test_every_file_follows_the_layout_rules


def test_reports_packed_blocks_and_missing_or_stray_markers(tmp_path: Path) -> None:
    sample = tmp_path / "sample.py"
    sample.write_text(
        "def packed(value: int) -> int:\n"
        "    if value:\n"
        "        value += 1\n"
        "    # a comment against the block\n"
        "    return value\n"
        "\n"
        "\n"
        "class Marked:\n"
        "    def method(self) -> None:\n"
        "        pass\n"
        "    # end function method\n"
        "\n"
        "\n"
        "# end class Marked\n"
        "# end function gone\n"
    )

    assert layout_violations(sample, "sample.py") == [
        "sample.py:4: leave a blank line after the block that starts on line 2",
        "sample.py:1: end packed with '# end function packed' on the line after "
        "its body, at column 0",
        "sample.py:9: end method with '# end method method' on the line after its "
        "body, at column 4",
        "sample.py:11: this end marker belongs to no definition",
        "sample.py:15: this end marker belongs to no definition",
    ]


# end function test_reports_packed_blocks_and_missing_or_stray_markers
