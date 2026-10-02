# Conventions

Simplicity and maintainability are paramount. These rules bind every change; the surface contract lives in the specifications repository, and the JavaScript client is the reference implementation this package mirrors.

## Descriptive names

Use full domain words: `release_message_interest`, `flush_interests`, `segment_id`, `credential_provider`. No abbreviations, and no single-letter names outside tight loops. A name says what the thing is; a comment exists only to state a constraint the code cannot show.

## Simplicity over abstraction

Solve the problem in front of you with the simplest structure that stays readable. No registries, factories, event frameworks, dependency-injection containers or wrapper layers. A helper class earns its place only by removing real, present duplication (`_ListenerSet` qualifies; a "Manager" does not). Prefer a function over a class, and a method on an existing class over a new class.

Two seams exist because Python needs them, and no others should appear: `_websocket.WebSocket`, a browser-style socket over the `websockets` library, so the connection logic matches the reference; and `Timers`, so tests can drive time.

## Maintainability

- Small modules with one responsibility; the file name states it. Modules are private (`_name.py`); `__init__.py` is the only place that re-exports, and the public surface is exactly its `__all__`.
- Import a name from the module that defines it.
- Every fixed value lives in `_constants.py`, in `SCREAMING_SNAKE_CASE`. Validation schemas and patterns stay beside the code that uses them.
- Delete code in the same change that obsoletes it.
- Every public identifier traces to a requirement or a recorded decision in the specifications repository (SEG-01, DEV-01, REV-01, ...).
- Errors carry stable codes and messages that name what failed, where, and which rule or limit it broke. Never interpolate received values, input values, credentials or server text. Never chain a caught exception that may hold such values: raise outside the handler, or after `contextlib.suppress`, so `__context__` stays empty.
- Validation uses Pydantic in strict mode (`validate_input`); nothing is coerced.
- Tests are deterministic (injected clocks and randomness, `FakeTimers`), grouped by behaviour, and catch package-owned defects only. Golden vectors are hand-authored, never produced by the code under test.
- Before completion, review the full diff for anything deletable without weakening behaviour or tests.

## Layout

Leave one blank line after every block (`if`, `for`, `while`, `with`, `try`, `match`) before the next statement, except before `elif`, `else`, `except` or `finally`, or at the end of an enclosing block. Code packed against the block before it is harder to read, and is treated as a defect in review.

`ruff format` owns everything else about layout; `ruff check` and `mypy --strict` must pass.
