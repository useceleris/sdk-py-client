# Conventions

Simplicity and maintainability are paramount. These rules bind every change; the surface contract lives in the specifications repository, and the JavaScript client is the reference implementation this package mirrors.

## Descriptive names

Use full domain words: `release_message_interest`, `flush_interests`, `segment_id`, `credential_provider`. No abbreviations, and no single-letter names outside tight loops. A name says what the thing is; a comment exists only to state a constraint the code cannot show.

## Simplicity over abstraction

Solve the problem in front of you with the simplest structure that stays readable. No registries, factories, event frameworks, dependency-injection containers or wrapper layers. A helper class earns its place only by removing real, present duplication (`_ListenerSet` qualifies; a "Manager" does not). Prefer a function over a class, and a method on an existing class over a new class.

Two seams exist because Python needs them, and no others should appear: `_websocket.WebSocket`, a browser-style socket over the `websockets` library, so the connection logic matches the reference; and `Timers`, so tests can drive time.

`_websocket.WebSocket` is also the only place that touches threads. Each socket runs its connection on its own daemon thread and event loop, so pings keep flowing while a listener holds the caller's loop (HEARTBEAT-01). Everything it reports runs on the caller's loop, handed over with `call_soon_threadsafe`, one event at a time: the thread reads the next frame only after the previous one was delivered (DEV-01). Its state shared with the thread sits behind one lock, and the thread is joined before `on_close` runs. No other module creates threads or touches another loop.

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

Leave one blank line after every compound statement (`if`, `for`, `while`, `with`, `try`, `match`, `def`, `class`) before the next statement in the same block. `elif`, `else`, `except` and `finally` belong to their statement, and the end of an enclosing block needs no blank line. Code packed against the block before it is harder to read.

Every function, method and class ends with a marker comment that names it, as the first line after its body at the definition's own indentation: `# end function name` at module level or inside a function, `# end method name` inside a class, and `# end class Name`. `ruff format` puts blank lines before the marker; nothing else may come between. Ruff treats `end` as a task tag, so a marker may run past the line length.

```python
def outer(value: int) -> int:
    def inner() -> int:
        return value

    # end function inner

    return inner()


# end function outer
```

`tests/test_layout.py` enforces both rules over `src`, `tests`, `examples` and `noxfile.py`, and names the file and line of each violation. `ruff format` owns everything else about layout; `ruff check` and `mypy --strict` must pass. `uv run nox` runs all of them, and CI runs `uv run --locked nox`.
