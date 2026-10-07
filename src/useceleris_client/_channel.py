import asyncio
import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Generic, Literal, TypeAlias

from typing_extensions import ParamSpec

from useceleris_client._command_queue import (
    CommandQueue,
    CommandQueueDelegates,
    InterestKind,
)
from useceleris_client._commands import IDENTIFIER, InterestCommand
from useceleris_client._connection import ConnectionHandle, open_connection
from useceleris_client._constants import (
    CLOSE_BUDGET_MS,
    DEFAULT_SEGMENT_ID,
    PRESENCE_LIST_COMMAND,
    RATE_LIMIT_ERROR_TYPE,
    RETRY_BUDGET_RESET_MS,
)
from useceleris_client._credential_types import CredentialProvider
from useceleris_client._credentials import Recovery
from useceleris_client._encode import encode_client_command
from useceleris_client._errors import (
    CelerisConnectionError,
    ConfigurationError,
    ProtocolError,
    ServerError,
)
from useceleris_client._messages import (
    ArrayFrame,
    ErrorFrame,
    MessageFrame,
    NoticeFrame,
    PresenceConnection,
    PresenceListFrame,
    PresenceNotifyFrame,
    ServerMessage,
)
from useceleris_client._parse_error import validate_input
from useceleris_client._reconnect import (
    compute_replay_lookback_ms,
    compute_retry_delay_ms,
)
from useceleris_client._segment import Segment, SegmentDelegates
from useceleris_client._timers import Timer, Timers

ChannelError: TypeAlias = (
    ConfigurationError | CelerisConnectionError | ProtocolError | ServerError
)

ChannelState: TypeAlias = Literal[
    "idle", "connecting", "connected", "reconnecting", "failed", "closing", "closed"
]


@dataclass(frozen=True)
class MessageMetadata:
    token_reference: str
    segment_id: str
    # Always present: the publisher's or the server's (REV-01).
    message_id: str
    timestamp: int


# Payload first so decoding composes; everything else arrives beside it.
MessageListener: TypeAlias = Callable[[bytes, MessageMetadata], None]


@dataclass(frozen=True)
class ServerNotice:
    timestamp: int
    payload: bytes


@dataclass(frozen=True)
class PresencePage:
    segment_id: str
    total: int
    per_page: int
    current_page: int
    # from_ > to is possible: raw metadata is preserved.
    from_: int
    to: int
    connections: tuple[PresenceConnection, ...]


@dataclass(frozen=True)
class PresenceEvent:
    """One connection joining or leaving one segment (PRES-01). Delivered only
    where subscribe_presence() is held, because the server fans these out to
    presence subscribers alone."""

    segment_id: str
    token_reference: str
    connection_id: str
    # False when the connection left.
    joined: bool
    timestamp: int


@dataclass(frozen=True)
class RecoveryEvent:
    retry_index: int
    possible_gaps: Literal[True] = True
    possible_duplicates: Literal[True] = True


class Subscription:
    """Holds one interest in a segment until cancelled."""

    def __init__(self, release: Callable[[], None]) -> None:
        self._release = release
        self._cancelled = False

    def cancel(self) -> None:
        """Idempotent."""
        if self._cancelled:
            return

        self._cancelled = True
        self._release()


Listener = ParamSpec("Listener")


class _ListenerEntry(Generic[Listener]):
    # One per registration, so a callback registered twice is two entries.
    def __init__(self, callback: Callable[Listener, object]) -> None:
        self.callback = callback


class _ListenerSet(Generic[Listener]):
    """Synchronous dispatch with no queue (DEV-01): a slow listener blocks
    dispatch rather than growing a backlog."""

    def __init__(self, contain_failure: Callable[[], None]) -> None:
        self._contain_failure = contain_failure
        self._entries: list[_ListenerEntry[Listener]] = []

    def add(self, callback: Callable[Listener, object]) -> Callable[[], None]:
        # A coroutine function's call would only create a coroutine that never
        # runs, so it is refused here rather than silently dropped.
        if not callable(callback) or inspect.iscoroutinefunction(callback):
            raise ConfigurationError(
                "Invalid listener. It must be a synchronous callable; start a task "
                "from it for asynchronous work."
            )

        entry = _ListenerEntry(callback)
        self._entries.append(entry)

        def dispose() -> None:
            if entry in self._entries:
                self._entries.remove(entry)

        return dispose

    def dispatch(self, *values: Listener.args, **named: Listener.kwargs) -> None:
        # A snapshot, so listeners added during dispatch wait for the next
        # event, and a membership check, so one disposed mid-dispatch is
        # skipped.
        for entry in list(self._entries):
            if entry not in self._entries:
                continue

            # CancelledError too: raised inside a synchronous callback it is
            # the callback's own failure, never a cancellation of the reader.
            try:
                outcome = entry.callback(*values, **named)
            except (Exception, asyncio.CancelledError):
                self._contain_failure()
                continue

            # A coroutine handed back would never run: it fails like a raising
            # listener, and is closed so it is not reported as never awaited.
            if inspect.iscoroutine(outcome):
                outcome.close()
                self._contain_failure()


class ChannelEventHandler:
    """Channel-level events. Each registration returns a disposer that removes
    exactly that listener."""

    def __init__(
        self,
        state_listeners: _ListenerSet[[ChannelState]],
        recovery_listeners: _ListenerSet[[RecoveryEvent]],
        notice_listeners: _ListenerSet[[ServerNotice]],
        error_listeners: _ListenerSet[[ChannelError]],
        message_listeners: _ListenerSet[[bytes, MessageMetadata]],
    ) -> None:
        self._state_listeners = state_listeners
        self._recovery_listeners = recovery_listeners
        self._notice_listeners = notice_listeners
        self._error_listeners = error_listeners
        self._message_listeners = message_listeners

    def on_state_change(
        self, listener: Callable[[ChannelState], None]
    ) -> Callable[[], None]:
        return self._state_listeners.add(listener)

    def on_recovery(
        self, listener: Callable[[RecoveryEvent], None]
    ) -> Callable[[], None]:
        return self._recovery_listeners.add(listener)

    def on_notice(self, listener: Callable[[ServerNotice], None]) -> Callable[[], None]:
        return self._notice_listeners.add(listener)

    def on_error(self, listener: Callable[[ChannelError], None]) -> Callable[[], None]:
        return self._error_listeners.add(listener)

    def on_message(self, listener: MessageListener) -> Callable[[], None]:
        return self._message_listeners.add(listener)


@dataclass(frozen=True)
class ChannelInternals:
    base_url: str
    channel_reference: str
    allow_insecure_loopback: bool
    connect_timeout_ms: int
    reconnect_timeout_ms: int
    presence_query_timeout_ms: int
    publish_queue_size: int
    deduplication_window_size: int
    maximum_reconnect_attempts: int
    credential_provider: CredentialProvider
    clock: Callable[[], float]
    wall_clock: Callable[[], int]
    random: Callable[[], float]
    generate_message_id: Callable[[], str]
    timers: Timers


@dataclass(frozen=True)
class _Outage:
    disconnected_at: int
    started_monotonic: float


@dataclass(frozen=True)
class _PendingPresenceQuery:
    request_id: str
    answer: "asyncio.Future[PresencePage]"
    timer: Timer


class _DedupWindow:
    def __init__(self, window_size: int) -> None:
        self._window_size = window_size
        # Insertion-ordered, so the first key is the oldest.
        self._identifiers: dict[str, None] = {}

    def record_if_new(self, identifier: str) -> bool:
        """False for an identifier already in the window. A new one is
        recorded, evicting the oldest once the window is full."""
        if identifier in self._identifiers:
            return False

        self._identifiers[identifier] = None

        if len(self._identifiers) > self._window_size:
            del self._identifiers[next(iter(self._identifiers))]

        return True

    def clear(self) -> None:
        self._identifiers.clear()


class Channel:
    """One WebSocket client. Every segment of the channel is multiplexed over
    its single connection (SEG-01)."""

    def __init__(self, internals: ChannelInternals) -> None:
        self._internals = internals
        self._state: ChannelState = "idle"
        self._generation = 0
        self._handle: ConnectionHandle | None = None
        self._retries_used = 0
        self._outage: _Outage | None = None
        self._connected_at_monotonic = 0.0
        self._retry_timer: Timer | None = None
        self._reconnect_attempt: asyncio.Task[None] | None = None
        self._close_budget_timer: Timer | None = None
        self._attempt_cancellation: asyncio.Future[None] | None = None
        self._closed: asyncio.Future[None] | None = None
        self._dispatching_errors = False
        self._command_queue = CommandQueue(
            CommandQueueDelegates(
                handle=lambda: self._handle,
                interest_command=self._interest_command,
                receive_interest_write_failure=self._receive_interest_write_failure,
                clock=internals.clock,
                random=internals.random,
                timers=internals.timers,
            ),
            publish_queue_size=internals.publish_queue_size,
        )
        self._dedup_window = _DedupWindow(internals.deduplication_window_size)
        self._message_listeners: dict[str, _ListenerSet[[bytes, MessageMetadata]]] = {}
        self._presence_listeners: dict[str, _ListenerSet[[PresenceEvent]]] = {}
        # Segment -> interest count, in first-registration order.
        self._message_interests: dict[str, int] = {}
        self._presence_interests: dict[str, int] = {}
        # Issues presence query request ids. Kept per channel rather than per
        # socket, so an id is never reused across reconnects (QUERY-01).
        self._presence_request_count = 0
        self._pending_presence_query: _PendingPresenceQuery | None = None
        self._state_listeners: _ListenerSet[[ChannelState]] = _ListenerSet(
            self._report_listener_failure
        )
        self._recovery_listeners: _ListenerSet[[RecoveryEvent]] = _ListenerSet(
            self._report_listener_failure
        )
        # Error-listener failures are swallowed: reporting them would re-enter
        # error dispatch (_emit_error also guards against that re-entry).
        self._error_listeners: _ListenerSet[[ChannelError]] = _ListenerSet(lambda: None)
        self._notice_listeners: _ListenerSet[[ServerNotice]] = _ListenerSet(
            self._report_listener_failure
        )
        self._channel_message_listeners: _ListenerSet[[bytes, MessageMetadata]] = (
            _ListenerSet(self._report_listener_failure)
        )
        self._handler = ChannelEventHandler(
            self._state_listeners,
            self._recovery_listeners,
            self._notice_listeners,
            self._error_listeners,
            self._channel_message_listeners,
        )
        self._segment_delegates = SegmentDelegates(
            add_message_listener=self._add_message_listener,
            add_presence_listener=self._add_presence_listener,
            add_message_interest=lambda segment_id: self._add_interest(
                "message", segment_id
            ),
            add_presence_interest=lambda segment_id: self._add_interest(
                "presence", segment_id
            ),
            publish_to_segment=self._publish_to_segment,
            query_presence=self._query_presence,
        )

    @property
    def state(self) -> ChannelState:
        return self._state

    def events(self) -> ChannelEventHandler:
        return self._handler

    def segment(self, segment_id: str) -> Segment:
        """Side-effect free: a proxy over this channel's connection."""
        validate_input(IDENTIFIER, segment_id, "segment ID")
        return Segment(segment_id, self._segment_delegates)

    def default_segment(self) -> Segment:
        """The segment every connection joins automatically (SEG-01)."""
        return self.segment(DEFAULT_SEGMENT_ID)

    async def connect(self) -> None:
        """Returns once the socket is open. Cancelling the caller abandons the
        attempt and leaves the channel failed."""
        if self._state in ("connecting", "connected", "reconnecting"):
            raise CelerisConnectionError(
                "OperationInProgress",
                f"connect() was already called; the channel is {self._state}.",
            )

        if self._state in ("closing", "closed"):
            raise CelerisConnectionError(
                "NotConnected",
                "Channel is closed; create a new one with client.channel().",
            )

        self._generation += 1
        generation = self._generation
        self._retries_used = 0
        self._outage = None
        self._dedup_window.clear()
        self._set_state("connecting")

        try:
            await self._establish_connection({"reason": "initial"})
        except BaseException:
            if generation == self._generation:
                self._generation += 1
                self._set_state("failed")

            raise

        if generation == self._generation:
            self._set_state("connected")

    async def close(self) -> None:
        """Idempotent and terminal. Every call waits for the same close."""
        if self._closed is None:
            self._closed = asyncio.get_running_loop().create_future()
            self._begin_close()

        await asyncio.shield(self._closed)

    def _begin_close(self) -> None:
        self._reject_pending_presence_query(
            CelerisConnectionError(
                "Cancelled", "Channel closed while the presence query was pending."
            )
        )
        self._generation += 1
        self._clear_retry_timer()
        self._cancel_attempt()
        self._command_queue.reset(
            CelerisConnectionError(
                "Cancelled", "Channel closed before the publish was sent."
            )
        )

        handle = self._detach_handle()
        self._set_state("closing")

        if handle is None:
            self._finish_close()
            return

        handle.close()

        if self._state != "closed":
            self._close_budget_timer = self._internals.timers.call_later(
                CLOSE_BUDGET_MS, self._finish_close
            )

    def _finish_close(self) -> None:
        if self._state == "closed":
            return

        if self._close_budget_timer is not None:
            self._close_budget_timer.cancel()
            self._close_budget_timer = None

        self._set_state("closed")

        if self._closed is not None and not self._closed.done():
            self._closed.set_result(None)

    def _cancel_attempt(self) -> None:
        if (
            self._attempt_cancellation is not None
            and not self._attempt_cancellation.done()
        ):
            self._attempt_cancellation.set_result(None)

        self._attempt_cancellation = None

    # Detach the socket reference before acting on it so no event delivered
    # during the follow-up can re-enter through a stale handle.
    def _detach_handle(self) -> ConnectionHandle | None:
        handle = self._handle
        self._handle = None
        return handle

    async def _establish_connection(self, recovery: Recovery) -> None:
        """Opens the WebSocket, installs it, and re-sends every subscription."""
        generation = self._generation
        cancellation: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._attempt_cancellation = cancellation

        try:
            handle = await open_connection(
                {
                    "base_url": self._internals.base_url,
                    "channel_reference": self._internals.channel_reference,
                    "allow_insecure_loopback": self._internals.allow_insecure_loopback,
                    "recovery": recovery,
                },
                credential_provider=self._internals.credential_provider,
                on_message=lambda message: self._route_message(generation, message),
                on_close=lambda: self._receive_socket_close(generation),
                on_error=lambda error: self._receive_socket_error(generation, error),
                timeout_ms=(
                    self._internals.reconnect_timeout_ms
                    if recovery["reason"] == "reconnect"
                    else self._internals.connect_timeout_ms
                ),
                timers=self._internals.timers,
                cancellation=cancellation,
            )

            if generation != self._generation:
                handle.close()
                raise CelerisConnectionError(
                    "Cancelled",
                    "Connection attempt cancelled: the channel was closed or a "
                    "newer attempt started.",
                )

            # Its close, had it come before now, found the channel not yet
            # connected and was ignored.
            if not handle.is_open:
                raise CelerisConnectionError(
                    "Transport", "The WebSocket closed as soon as it opened."
                )

            self._handle = handle
            self._connected_at_monotonic = self._internals.clock()
            self._outage = None
            self._flush_interests()

            # A refused subscription write detaches the socket it was sent on.
            if self._handle is not handle:
                raise CelerisConnectionError(
                    "Transport",
                    "Restoring subscriptions failed: the socket refused a write.",
                )
        except BaseException:
            # Waiting publishes stay for the next attempt (QUEUE-01).
            self._command_queue.reset_connection_state()
            raise
        finally:
            if self._attempt_cancellation is cancellation:
                self._attempt_cancellation = None

    def _add_message_listener(
        self, segment_id: str, listener: Callable[[bytes, MessageMetadata], None]
    ) -> Callable[[], None]:
        listeners = self._message_listeners.get(segment_id)

        if listeners is None:
            listeners = _ListenerSet(self._report_listener_failure)
            self._message_listeners[segment_id] = listeners

        return listeners.add(listener)

    def _add_presence_listener(
        self, segment_id: str, listener: Callable[[PresenceEvent], None]
    ) -> Callable[[], None]:
        listeners = self._presence_listeners.get(segment_id)

        if listeners is None:
            listeners = _ListenerSet(self._report_listener_failure)
            self._presence_listeners[segment_id] = listeners

        return listeners.add(listener)

    # Ref-counts one interest; the first registration and the last
    # cancellation queue a sync of that segment's subscription.
    def _add_interest(self, kind: InterestKind, segment_id: str) -> Subscription:
        if self._state in ("closing", "closed"):
            raise CelerisConnectionError(
                "NotConnected",
                "Channel is closed; create a new one with client.channel().",
            )

        interests = (
            self._message_interests if kind == "message" else self._presence_interests
        )
        count = interests.get(segment_id, 0) + 1
        interests[segment_id] = count

        if count == 1:
            self._queue_interest_sync(kind, segment_id)

        def release() -> None:
            remaining = interests.get(segment_id, 1) - 1

            if remaining > 0:
                interests[segment_id] = remaining
                return

            interests.pop(segment_id, None)
            self._queue_interest_sync(kind, segment_id)

        return Subscription(release)

    # Without a socket there is nothing to sync: installing one syncs every
    # held interest.
    def _queue_interest_sync(self, kind: InterestKind, segment_id: str) -> None:
        if self._handle is not None:
            self._command_queue.queue_interest(kind, segment_id)

    # The command that brings the server in line with the segment's interest
    # as it stands now. Subscriptions are synced as state, so a resend is
    # always safe.
    def _interest_command(
        self, kind: InterestKind, segment_id: str
    ) -> InterestCommand | None:
        # Presence applies to every segment, the default one included: the
        # server's connect-time auto-join grants message membership only.
        if kind == "presence":
            return {
                "command": (
                    "PRES_SUB"
                    if segment_id in self._presence_interests
                    else "PRES_UNSUB"
                ),
                "segment_id": segment_id,
            }

        # The server joins the default segment on connect and never leaves it.
        if segment_id == DEFAULT_SEGMENT_ID:
            return None

        # Watching presence is not membership, so it never holds the segment.
        return {
            "command": "SUB" if segment_id in self._message_interests else "UNSUB",
            "segment_id": segment_id,
        }

    # The server's view of this connection's subscriptions is now unknown, so
    # the socket is replaced: reconnecting re-sends every subscription.
    def _receive_interest_write_failure(self) -> None:
        handle = self._detach_handle()

        if self._state == "connected":
            self._enter_reconnecting()

        if handle is not None:
            handle.close()

    async def _query_presence(
        self, segment_id: str, page: int, per_page: int
    ) -> PresencePage:
        handle = self._handle

        if self._state != "connected" or handle is None:
            raise CelerisConnectionError(
                "NotConnected", f"Channel is not connected; it is {self._state}."
            )

        if self._pending_presence_query is not None:
            raise CelerisConnectionError(
                "OperationInProgress",
                "A presence query is already in flight; wait for it to settle "
                "before starting another.",
            )

        self._presence_request_count += 1
        request_id = str(self._presence_request_count)
        data = encode_client_command(
            {
                "command": "PRES_LIST",
                "segment_id": segment_id,
                "page": page,
                "per_page": per_page,
                "request_id": request_id,
            }
        )

        # A send failure raises without ever taking the query slot.
        self._command_queue.send_now(handle, data)

        # A timed-out or cancelled query frees its slot and leaves the
        # connection alone: a late reply carries the old request id and is
        # dropped.
        answer: asyncio.Future[PresencePage] = (
            asyncio.get_running_loop().create_future()
        )
        timeout_ms = self._internals.presence_query_timeout_ms
        pending = _PendingPresenceQuery(
            request_id=request_id,
            answer=answer,
            timer=self._internals.timers.call_later(
                timeout_ms,
                lambda: self._reject_pending_presence_query(
                    CelerisConnectionError(
                        "Timeout", f"Presence query timed out after {timeout_ms} ms."
                    )
                ),
            ),
        )
        self._pending_presence_query = pending

        try:
            return await answer
        except asyncio.CancelledError:
            if self._pending_presence_query is pending:
                self._take_pending_presence_query()

            raise

    def _take_pending_presence_query(self) -> _PendingPresenceQuery | None:
        pending = self._pending_presence_query

        if pending is None:
            return None

        self._pending_presence_query = None
        pending.timer.cancel()
        return pending

    def _reject_pending_presence_query(self, error: ChannelError) -> None:
        pending = self._take_pending_presence_query()

        if pending is not None and not pending.answer.done():
            pending.answer.set_exception(error)

    def _reject_presence_query_on_connection_loss(self) -> None:
        self._reject_pending_presence_query(
            CelerisConnectionError(
                "Transport",
                "Connection lost during the presence query; query again once the "
                "channel reconnects.",
            )
        )

    async def _publish_to_segment(
        self, segment_id: str, payload: bytes, message_id: str | None
    ) -> None:
        # While reconnecting, the publish waits in the queue for the next
        # socket (QUEUE-01). Anywhere else no recovery is in progress.
        if self._state not in ("connected", "reconnecting"):
            raise CelerisConnectionError(
                "NotConnected", f"Channel is not connected; it is {self._state}."
            )

        data = encode_client_command(
            {
                "command": "PUB",
                "segment_id": segment_id,
                "message_id": (
                    message_id
                    if message_id is not None
                    else self._internals.generate_message_id()
                ),
                "payload": payload,
            }
        )

        await self._command_queue.publish(segment_id, data)

    # Restoration goes through the queue, so it waits for writer room and
    # reaches the server before any publish, messages first, then presence.
    def _flush_interests(self) -> None:
        self._command_queue.restore_interests(
            [("message", segment_id) for segment_id in self._message_interests]
            + [("presence", segment_id) for segment_id in self._presence_interests]
        )

    def _route_message(self, attempt_generation: int, message: ServerMessage) -> None:
        if attempt_generation != self._generation:
            return

        match message:
            case ArrayFrame():
                for entry in message.messages:
                    self._route_message(attempt_generation, entry)
            case MessageFrame():
                self._deliver_message(message)
            case ErrorFrame():
                self._receive_error_frame(message)
            case NoticeFrame():
                self._notice_listeners.dispatch(
                    ServerNotice(timestamp=message.timestamp, payload=message.payload)
                )
            case PresenceNotifyFrame():
                self._deliver_presence(message)
            case PresenceListFrame():
                self._receive_presence_response(message)
            case _:
                return

    def _receive_error_frame(self, frame: ErrorFrame) -> None:
        # The server never closes the socket on an error frame; report it
        # once, every field as sent, and remain connected (ERR-01). Decoding
        # the message replaces malformed bytes: delivering an error must never
        # itself fail.
        error = ServerError(
            frame.type,
            frame.sub_type,
            frame.message.decode(errors="replace").removeprefix("﻿"),
            frame.resource,
        )

        # A presence query error names its query by request id and answers
        # that query alone. A stale id belongs to a query that was already
        # rejected and reported, so it is dropped (QUERY-01).
        if frame.sub_type == PRESENCE_LIST_COMMAND:
            pending = self._pending_presence_query

            if pending is not None and frame.resource == pending.request_id:
                self._reject_pending_presence_query(error)

            return

        if frame.type == RATE_LIMIT_ERROR_TYPE:
            self._command_queue.receive_rate_limit()

        self._emit_error(error)

    def _receive_presence_response(self, response: PresenceListFrame) -> None:
        pending = self._pending_presence_query

        # A response carrying any other request id answers a query that
        # already failed, so it is dropped; a pending query keeps waiting for
        # its own.
        if pending is None or response.request_id != pending.request_id:
            return

        self._take_pending_presence_query()

        if not pending.answer.done():
            pending.answer.set_result(
                PresencePage(
                    segment_id=response.segment_id,
                    total=response.total,
                    per_page=response.per_page,
                    current_page=response.current_page,
                    from_=response.from_,
                    to=response.to,
                    connections=response.connections,
                )
            )

    def _deliver_message(self, message: MessageFrame) -> None:
        # REV-01: every MSG carries an id, the publisher's or one the server
        # assigns. A missing id leaves the message undeliverable, since it
        # cannot be deduplicated, so it is dropped and reported without taking
        # the connection down with it (DECODE-01).
        if message.message_id is None:
            self._emit_error(
                ProtocolError(
                    "Server message is missing its identifier.", "message_id", 0
                )
            )
            return

        # Ids are recorded before fanout, even with no listeners.
        if not self._dedup_window.record_if_new(message.message_id):
            return

        metadata = MessageMetadata(
            token_reference=message.token_reference,
            segment_id=message.segment_id,
            message_id=message.message_id,
            timestamp=message.timestamp,
        )
        segment_listeners = self._message_listeners.get(message.segment_id)

        # The segment's listeners first, then the channel's (MSG-02).
        if segment_listeners is not None:
            segment_listeners.dispatch(message.payload, metadata)

        self._channel_message_listeners.dispatch(message.payload, metadata)

    def _deliver_presence(self, event: PresenceNotifyFrame) -> None:
        listeners = self._presence_listeners.get(event.segment_id)

        if listeners is None:
            return

        listeners.dispatch(
            PresenceEvent(
                segment_id=event.segment_id,
                token_reference=event.token_reference,
                connection_id=event.connection_id,
                joined=event.joined,
                timestamp=event.timestamp,
            )
        )

    def _receive_socket_close(self, attempt_generation: int) -> None:
        if self._state == "closing":
            self._finish_close()
            return

        if attempt_generation != self._generation or self._state != "connected":
            return

        self._enter_reconnecting()

    def _receive_socket_error(
        self,
        attempt_generation: int,
        error: CelerisConnectionError | ProtocolError,
    ) -> None:
        if attempt_generation != self._generation or self._state != "connected":
            return

        if isinstance(error, ProtocolError):
            # A frame that could not be decoded was dropped by the connection
            # layer, which left the socket open. Report it and stay connected:
            # decoding never spans frames, so the next one is unaffected
            # (DECODE-01).
            self._emit_error(error)
            return

        self._enter_reconnecting()

    def _enter_reconnecting(self) -> None:
        self._reject_presence_query_on_connection_loss()
        self._command_queue.reset_connection_state()
        now = self._internals.clock()

        if now - self._connected_at_monotonic >= RETRY_BUDGET_RESET_MS:
            self._retries_used = 0

        self._outage = _Outage(
            disconnected_at=self._internals.wall_clock(), started_monotonic=now
        )
        self._handle = None
        self._set_state("reconnecting")
        self._schedule_retry()

    def _schedule_retry(self) -> None:
        generation = self._generation
        delay = compute_retry_delay_ms(self._retries_used, self._internals.random)

        def retry() -> None:
            self._retry_timer = None

            if generation != self._generation:
                return

            self._reconnect_attempt = asyncio.get_running_loop().create_task(
                self._run_reconnect_attempt(generation)
            )

        self._retry_timer = self._internals.timers.call_later(delay, retry)

    # The attempt starts a loop iteration after its timer fired, so it checks
    # the timer's generation: a close in between ends it before it begins.
    async def _run_reconnect_attempt(self, generation: int) -> None:
        outage = self._outage

        if generation != self._generation or outage is None:
            return

        attempt_index = self._retries_used

        try:
            await self._establish_connection(
                {
                    "reason": "reconnect",
                    "disconnected_at": outage.disconnected_at,
                    "replay_lookback_ms": compute_replay_lookback_ms(
                        self._internals.clock() - outage.started_monotonic
                    ),
                }
            )
        except Exception as error:
            if generation != self._generation:
                return

            if isinstance(error, CelerisConnectionError) and error.code in (
                "Transport",
                "Timeout",
            ):
                self._retries_used += 1

                if self._retries_used >= self._internals.maximum_reconnect_attempts:
                    self._fail_terminal(error)
                    return

                self._schedule_retry()
                return

            self._fail_terminal(
                error
                if isinstance(
                    error, (ConfigurationError, CelerisConnectionError, ProtocolError)
                )
                else CelerisConnectionError(
                    "Transport", "Reconnect attempt failed with an unexpected error."
                )
            )
            return

        if generation != self._generation:
            return

        self._set_state("connected")
        self._recovery_listeners.dispatch(RecoveryEvent(retry_index=attempt_index))

    # Waiting publishes fail with the error on_error reports (QUEUE-01).
    def _fail_terminal(self, error: ChannelError) -> None:
        self._reject_presence_query_on_connection_loss()
        self._command_queue.reset(error)
        self._generation += 1
        self._clear_retry_timer()
        self._emit_error(error)
        self._set_state("failed")

    def _clear_retry_timer(self) -> None:
        if self._retry_timer is not None:
            self._retry_timer.cancel()
            self._retry_timer = None

    def _set_state(self, state: ChannelState) -> None:
        self._state = state
        self._state_listeners.dispatch(state)

    def _report_listener_failure(self) -> None:
        self._emit_error(
            CelerisConnectionError(
                "Transport",
                "A listener callback raised an error; the channel caught it and "
                "kept running.",
            )
        )

    def _emit_error(self, error: ChannelError) -> None:
        if self._dispatching_errors:
            return

        self._dispatching_errors = True

        try:
            self._error_listeners.dispatch(error)
        finally:
            self._dispatching_errors = False
