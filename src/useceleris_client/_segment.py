from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from useceleris_client._channel import (
        MessageMetadata,
        PresenceEvent,
        PresencePage,
        Subscription,
    )


@dataclass(frozen=True)
class SegmentDelegates:
    add_message_listener: Callable[
        [str, Callable[[bytes, MessageMetadata], None]], Callable[[], None]
    ]
    add_presence_listener: Callable[
        [str, Callable[[PresenceEvent], None]], Callable[[], None]
    ]
    add_message_interest: Callable[[str], Subscription]
    add_presence_interest: Callable[[str], Subscription]
    publish_to_segment: Callable[[str, bytes, str | None], Awaitable[None]]
    query_presence: Callable[[str, int, int], Awaitable[PresencePage]]


# end class SegmentDelegates


class Segment:
    """A stateless proxy over its channel's single connection.

    It holds only its segment identifier and the channel's delegate functions.
    All connection state, interest counts and listeners live on the channel,
    so any number of Segment objects for one segment share them.
    """

    def __init__(self, segment_id: str, delegates: SegmentDelegates) -> None:
        self._segment_id = segment_id
        self._delegates = delegates

    # end method __init__

    @property
    def segment_id(self) -> str:
        return self._segment_id

    # end method segment_id

    def subscribe(self) -> Subscription:
        return self._delegates.add_message_interest(self._segment_id)

    # end method subscribe

    def on_message(
        self, listener: Callable[[bytes, MessageMetadata], None]
    ) -> Callable[[], None]:
        return self._delegates.add_message_listener(self._segment_id, listener)

    # end method on_message

    async def publish(self, payload: bytes, *, message_id: str | None = None) -> None:
        """Returns on local acceptance: once the socket has taken the command.
        While reconnecting, that is the new socket (QUEUE-01). An id is
        generated when none is given."""
        await self._delegates.publish_to_segment(self._segment_id, payload, message_id)

    # end method publish

    def subscribe_presence(self) -> Subscription:
        return self._delegates.add_presence_interest(self._segment_id)

    # end method subscribe_presence

    def on_presence(
        self, listener: Callable[[PresenceEvent], None]
    ) -> Callable[[], None]:
        # Events arrive only while subscribe_presence() is held: the server
        # fans them out to presence subscribers alone (PRES-01).
        return self._delegates.add_presence_listener(self._segment_id, listener)

    # end method on_presence

    async def presence_list(self, *, page: int, per_page: int) -> PresencePage:
        """One query in flight per channel; the request id is issued
        internally."""
        return await self._delegates.query_presence(self._segment_id, page, per_page)

    # end method presence_list


# end class Segment
