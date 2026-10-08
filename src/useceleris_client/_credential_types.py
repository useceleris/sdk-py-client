from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Literal, TypeAlias


@dataclass(frozen=True)
class Credentials:
    # Left out of repr so logging a value never prints a credential.
    payload: str = field(repr=False)
    signature: str = field(repr=False)


# end class Credentials


@dataclass(frozen=True)
class CredentialRequest:
    channel_reference: str
    reason: Literal["initial", "reconnect"]
    # Set on reconnect: when the connection was lost, in Unix milliseconds,
    # and how far back the new connection should replay.
    disconnected_at: int | None = None
    replay_lookback_ms: int | None = None


# end class CredentialRequest


# Called once per connection attempt, so credentials are always fresh
# (D-001). Cancelling the attempt cancels the call.
CredentialProvider: TypeAlias = Callable[[CredentialRequest], Awaitable[Credentials]]
