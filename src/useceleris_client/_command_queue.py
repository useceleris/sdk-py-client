import asyncio
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, TypeAlias

from useceleris_client._commands import InterestCommand
from useceleris_client._connection import ConnectionHandle
from useceleris_client._constants import (
    DRAIN_RETRY_MS,
    MAXIMUM_BATCH_BYTES,
    MAXIMUM_BATCH_COMMANDS,
    MAXIMUM_CONSECUTIVE_RATE_LIMITS,
    MAXIMUM_PENDING_COMMANDS,
    MAXIMUM_PUBLISH_RESENDS,
    QUOTA_PROBE_FIRST_DELAY_MS,
    QUOTA_PROBE_MAXIMUM_DELAY_MS,
    QUOTA_RETURN_CONFIRMATION_MS,
    RATE_LIMIT_COOLDOWN_MS,
    RATE_LIMIT_SUSPECT_WINDOW_MS,
)
from useceleris_client._encode import encode_batch, encode_client_command
from useceleris_client._errors import (
    CelerisConnectionError,
    CelerisError,
    ConfigurationError,
)
from useceleris_client._reconnect import compute_retry_delay_ms
from useceleris_client._timers import Timer, Timers

InterestKind: TypeAlias = Literal["message", "presence"]

_INTEREST_KINDS: tuple[InterestKind, ...] = ("message", "presence")


@dataclass(frozen=True)
class CommandQueueDelegates:
    handle: Callable[[], ConnectionHandle | None]
    # The command that brings the server in line with the segment's interest
    # as it stands now, or None when nothing needs sending.
    interest_command: Callable[[InterestKind, str], InterestCommand | None]
    # The socket refused a subscription write, so the server's view of this
    # connection's subscriptions is unknown.
    receive_interest_write_failure: Callable[[], None]
    clock: Callable[[], float]
    random: Callable[[], float]
    timers: Timers


# end class CommandQueueDelegates


@dataclass(eq=False)
class _QueuedPublish:
    segment_id: str
    message_id: str
    data: bytes
    # Its place in the order commands were queued in.
    sequence: int
    # Completes the caller's publish; done once settled.
    sent: "asyncio.Future[None]"
    resends: int = 0


# end class _QueuedPublish


@dataclass(frozen=True)
class _SentInterest:
    kind: InterestKind
    segment_id: str
    sent_at: float


# end class _SentInterest


@dataclass(frozen=True)
class _SentPublish:
    publish: _QueuedPublish
    sent_at: float


# end class _SentPublish


@dataclass(frozen=True)
class _PendingInterest:
    kind: InterestKind
    segment_id: str
    sequence: int


# end class _PendingInterest


@dataclass(frozen=True)
class _BatchedCommand:
    """A command taken for the frame being packed: a publish, or else a
    subscription change."""

    data: bytes
    publish: _QueuedPublish | None = None
    interest: _PendingInterest | None = None


# end class _BatchedCommand


def _settle(publish: _QueuedPublish, error: CelerisError | None = None) -> None:
    if publish.sent.done():
        return

    if error is None:
        publish.sent.set_result(None)
    else:
        publish.sent.set_exception(error)


# end function _settle


class CommandQueue:
    """Sends subscription changes ahead of publishes, packs the commands
    waiting when it sends into one frame (BATCH-01), waits out a full writer,
    and resends recent commands after a rate limit (RESEND-01).

    The server never says which frame a rate limit dropped, so everything sent
    within the suspect window is resent: subscriptions as their current state,
    publishes once and with their original message id, so receivers drop any
    copy that got through. Waiting publishes outlive a dropped socket and go
    out on the next one, after the restored subscriptions (QUEUE-01).
    """

    def __init__(
        self, delegates: CommandQueueDelegates, *, publish_queue_size: int
    ) -> None:
        self._delegates = delegates
        self._publish_queue_size = publish_queue_size
        # Segment -> the sequence of its latest change, in first-change order.
        self._pending_interests: dict[InterestKind, dict[str, int]] = {
            "message": {},
            "presence": {},
        }
        self._publishes: list[_QueuedPublish] = []
        self._sequence = 0
        self._recent_interests: deque[_SentInterest] = deque()
        # Holds payloads, so it keeps no more than MAXIMUM_PENDING_COMMANDS.
        self._recent_publishes: deque[_SentPublish] = deque()
        self._pending_commands = 0
        self._drain_timer: Timer | None = None
        self._pause_timer: Timer | None = None
        self._rate_limit_streak = 0
        self._rate_limit_streak_ends_at = 0.0
        # Subscriptions dropped while the limit was treated as a used-up
        # quota, re-sent by a probe on a slow, doubling schedule. Dicts keep
        # them in the order they were dropped.
        self._abandoned_interests: dict[InterestKind, dict[str, None]] = {
            "message": {},
            "presence": {},
        }
        self._probe_timer: Timer | None = None
        self._probe_count = 0
        self._first_sent_since_rate_limit_at: float | None = None

    # end method __init__

    def queue_interest(self, kind: InterestKind, segment_id: str) -> None:
        self._mark_interest(kind, segment_id)
        self._drain()

    # end method queue_interest

    async def publish(self, segment_id: str, message_id: str, data: bytes) -> None:
        """Returns once the publish is handed to the socket. Cancelling the
        caller withdraws a publish that has not gone out."""
        if len(self._publishes) >= self._publish_queue_size:
            raise CelerisConnectionError(
                "Backpressure",
                f"The publish queue is full (size {self._publish_queue_size}). "
                "Retry once some publishes have gone out.",
            )

        self._sequence += 1
        publish = _QueuedPublish(
            segment_id=segment_id,
            message_id=message_id,
            data=data,
            sequence=self._sequence,
            sent=asyncio.get_running_loop().create_future(),
        )
        self._publishes.append(publish)
        self._drain()

        try:
            await publish.sent
        except asyncio.CancelledError:
            if publish in self._publishes:
                self._publishes.remove(publish)

            raise

    # end method publish

    def send_now(self, handle: ConnectionHandle, data: bytes) -> None:
        """For commands that are never queued or resent, such as presence
        queries."""
        if self._pause_timer is not None:
            raise CelerisConnectionError(
                "Backpressure",
                "Sending is paused after a rate limit; try again in a moment.",
            )

        if not self._has_room(handle):
            raise CelerisConnectionError(
                "Backpressure",
                f"Command writer is full: {MAXIMUM_PENDING_COMMANDS} commands are "
                "waiting to be sent. Retry once the socket has flushed them.",
            )

        handle.send(data)
        self._pending_commands += 1

        if self._first_sent_since_rate_limit_at is None:
            self._first_sent_since_rate_limit_at = self._delegates.clock()

    # end method send_now

    def forget_refused(self, message_id: str) -> None:
        """The server refused this publish for its size, so a later rate limit
        must not send it, and bill it, again."""
        self._recent_publishes = deque(
            sent
            for sent in self._recent_publishes
            if sent.publish.message_id != message_id
        )

    # end method forget_refused

    def receive_rate_limit(self) -> None:
        # A limit arriving while sending is paused, with nothing sent since
        # the last one, reports the same episode through another limit type
        # (the server throttles each type separately). It carries nothing new.
        if (
            self._pause_timer is not None
            and self._first_sent_since_rate_limit_at is None
        ):
            return

        now = self._delegates.clock()
        self._end_probing_if_quota_returned(now, QUOTA_RETURN_CONFIRMATION_MS)

        # While probing the streak holds, so a probe's own limit cannot start
        # another full run of resends.
        if self._probe_count == 0 and now > self._rate_limit_streak_ends_at:
            self._rate_limit_streak = 0

        # A limit that keeps returning is a used-up quota rather than a burst,
        # and resending into it would never succeed.
        if self._rate_limit_streak < MAXIMUM_CONSECUTIVE_RATE_LIMITS:
            self._requeue_recent(now)
        else:
            self._abandon_recent()

        self._recent_interests.clear()
        self._recent_publishes.clear()
        self._first_sent_since_rate_limit_at = None

        delay = RATE_LIMIT_COOLDOWN_MS + compute_retry_delay_ms(
            self._rate_limit_streak, self._delegates.random
        )
        self._rate_limit_streak += 1
        self._rate_limit_streak_ends_at = now + delay + RATE_LIMIT_SUSPECT_WINDOW_MS

        if self._pause_timer is not None:
            self._pause_timer.cancel()

        self._pause_timer = self._delegates.timers.call_later(delay, self._end_pause)

    # end method receive_rate_limit

    def restore_interests(self, interests: list[tuple[InterestKind, str]]) -> None:
        """Restored subscriptions go ahead of every waiting publish, even one
        queued earlier to the same segment, so the connection is a member of its
        segments again before the publishes go out (QUEUE-01). Drains once all
        are marked, so no publish slips between them."""
        for kind, segment_id in interests:
            self._pending_interests[kind][segment_id] = 0

        self._drain()

    # end method restore_interests

    def reset(self, error: CelerisError) -> None:
        """Waiting publishes fail with the error, and the socket state is
        cleared."""
        self.reset_connection_state()
        publishes = self._publishes
        self._publishes = []

        for publish in publishes:
            _settle(publish, error)

    # end method reset

    def reset_connection_state(self) -> None:
        """Nothing tied to the lost socket carries over: the next socket
        re-syncs every subscription itself, and nothing it was handed is
        resent. Waiting publishes stay for the next socket. The rate-limit
        streak and the probe schedule stay too: a reconnect does not refill a
        quota."""
        for timer in (self._drain_timer, self._pause_timer, self._probe_timer):
            if timer is not None:
                timer.cancel()

        self._drain_timer = None
        self._pause_timer = None
        self._probe_timer = None

        for kind in _INTEREST_KINDS:
            self._pending_interests[kind].clear()
            self._abandoned_interests[kind].clear()

        self._recent_interests.clear()
        self._recent_publishes.clear()
        self._pending_commands = 0
        self._first_sent_since_rate_limit_at = None

    # end method reset_connection_state

    def _end_pause(self) -> None:
        self._pause_timer = None
        self._drain()

    # end method _end_pause

    # A newer change takes a newer sequence, so the sync follows every publish
    # queued before it.
    def _mark_interest(self, kind: InterestKind, segment_id: str) -> None:
        self._sequence += 1
        self._pending_interests[kind][segment_id] = self._sequence

    # end method _mark_interest

    def _requeue_recent(self, now: float) -> None:
        for sent_interest in self._recent_interests:
            if now - sent_interest.sent_at <= RATE_LIMIT_SUSPECT_WINDOW_MS:
                self._mark_interest(sent_interest.kind, sent_interest.segment_id)

        resent: list[_QueuedPublish] = []

        for sent_publish in self._recent_publishes:
            if (
                now - sent_publish.sent_at <= RATE_LIMIT_SUSPECT_WINDOW_MS
                and sent_publish.publish.resends < MAXIMUM_PUBLISH_RESENDS
            ):
                sent_publish.publish.resends += 1
                resent.append(sent_publish.publish)

        self._publishes = resent + self._publishes

    # end method _requeue_recent

    # Recent publishes are dropped; recent subscriptions wait for a probe.
    # Every subscription sent since the previous limit is handed to the probe,
    # however late the report: syncs are idempotent, so over-abandoning costs
    # at most a redundant frame, while missing one loses the subscription.
    def _abandon_recent(self) -> None:
        for sent_interest in self._recent_interests:
            self._abandoned_interests[sent_interest.kind][sent_interest.segment_id] = (
                None
            )

        abandoned = any(self._abandoned_interests[kind] for kind in _INTEREST_KINDS)

        if not abandoned or self._probe_timer is not None:
            return

        delay = min(
            QUOTA_PROBE_MAXIMUM_DELAY_MS,
            QUOTA_PROBE_FIRST_DELAY_MS * 2**self._probe_count,
        )
        self._probe_count += 1
        self._probe_timer = self._delegates.timers.call_later(delay, self._run_probe)

    # end method _abandon_recent

    def _run_probe(self) -> None:
        self._probe_timer = None
        self._restore_abandoned()
        self._drain()

    # end method _run_probe

    def _restore_abandoned(self) -> None:
        for kind in _INTEREST_KINDS:
            for segment_id in self._abandoned_interests[kind]:
                self._mark_interest(kind, segment_id)

            self._abandoned_interests[kind].clear()

    # end method _restore_abandoned

    # Commands that went `quiet_span_ms` without a rate limit following them
    # mean the quota is back: abandoned subscriptions are restored at once.
    def _end_probing_if_quota_returned(self, now: float, quiet_span_ms: float) -> None:
        if (
            self._probe_count == 0
            or self._first_sent_since_rate_limit_at is None
            or now - self._first_sent_since_rate_limit_at <= quiet_span_ms
        ):
            return

        if self._probe_timer is not None:
            self._probe_timer.cancel()

        self._probe_timer = None
        self._probe_count = 0
        self._rate_limit_streak = 0
        self._restore_abandoned()

    # end method _end_probing_if_quota_returned

    def _drain(self) -> None:
        handle = self._delegates.handle()

        if handle is None or self._pause_timer is not None:
            return

        self._end_probing_if_quota_returned(
            self._delegates.clock(), RATE_LIMIT_SUSPECT_WINDOW_MS
        )

        while self._send_batch(handle):
            pass

    # end method _drain

    # Sends the waiting commands as one frame. False when draining has to stop.
    def _send_batch(self, handle: ConnectionHandle) -> bool:
        if self._next_command() is None:
            return False

        if not self._has_room(handle):
            self._schedule_drain()
            return False

        batch = self._pack_batch(MAXIMUM_PENDING_COMMANDS - self._pending_commands)

        try:
            handle.send(encode_batch([command.data for command in batch]))
        except CelerisConnectionError as error:
            if error.code == "Backpressure":
                self._unpack_batch(batch)
                self._schedule_drain()
                return False

            return self._fail_batch(batch, error)
        except ConfigurationError as error:
            return self._fail_batch(batch, error)

        self._pending_commands += len(batch)

        if self._first_sent_since_rate_limit_at is None:
            self._first_sent_since_rate_limit_at = self._delegates.clock()

        for command in batch:
            if command.publish is not None:
                self._record_sent_publish(command.publish)
                _settle(command.publish)
            elif command.interest is not None:
                self._record_sent_interest(
                    command.interest.kind, command.interest.segment_id
                )

        return True

    # end method _send_batch

    # Takes the waiting commands for one frame, in order: at most `room` and
    # MAXIMUM_BATCH_COMMANDS of them, within MAXIMUM_BATCH_BYTES with the array
    # header. A first command larger than that goes alone.
    def _pack_batch(self, room: int) -> list[_BatchedCommand]:
        batch: list[_BatchedCommand] = []
        byte_length = 0

        while len(batch) < min(room, MAXIMUM_BATCH_COMMANDS):
            command = self._next_command()

            if command is None:
                break

            frame_length = len(f"*{len(batch) + 1}\n") + byte_length + len(command.data)

            if batch and frame_length > MAXIMUM_BATCH_BYTES:
                break

            if command.publish is not None:
                self._publishes.pop(0)
            elif command.interest is not None:
                del self._pending_interests[command.interest.kind][
                    command.interest.segment_id
                ]

            batch.append(command)
            byte_length += len(command.data)

        return batch

    # end method _pack_batch

    # The next command to send, left in place. A subscription change that
    # needs nothing sent is done here.
    def _next_command(self) -> _BatchedCommand | None:
        while (interest := self._next_ready_interest()) is not None:
            command = self._delegates.interest_command(
                interest.kind, interest.segment_id
            )

            if command is not None:
                return _BatchedCommand(
                    encode_client_command(command), interest=interest
                )

            del self._pending_interests[interest.kind][interest.segment_id]

        if not self._publishes:
            return None

        return _BatchedCommand(self._publishes[0].data, publish=self._publishes[0])

    # end method _next_command

    # The socket had no room for the frame, so its commands wait again.
    def _unpack_batch(self, batch: list[_BatchedCommand]) -> None:
        publishes: list[_QueuedPublish] = []

        for command in batch:
            if command.publish is not None:
                publishes.append(command.publish)
            elif command.interest is not None:
                interest = command.interest
                self._pending_interests[interest.kind][interest.segment_id] = (
                    interest.sequence
                )

        self._publishes = publishes + self._publishes

    # end method _unpack_batch

    # Every publish in a frame the socket refused fails with its error;
    # ConnectionHandle.send raises nothing but SDK errors, such as
    # DeliveryUnknown. A refused subscription change leaves the server's view
    # unknown, so draining stops.
    def _fail_batch(self, batch: list[_BatchedCommand], error: CelerisError) -> bool:
        for command in batch:
            if command.publish is not None:
                _settle(command.publish, error)

        if all(command.publish is not None for command in batch):
            return True

        self._delegates.receive_interest_write_failure()
        return False

    # end method _fail_batch

    # The first subscription change with no earlier publish to its segment
    # still queued: the commands for one segment keep their call order, so a
    # change follows the publishes queued before it (RESEND-01).
    def _next_ready_interest(self) -> _PendingInterest | None:
        for kind in _INTEREST_KINDS:
            for segment_id, sequence in self._pending_interests[kind].items():
                blocked = any(
                    publish.segment_id == segment_id and publish.sequence < sequence
                    for publish in self._publishes
                )

                if not blocked:
                    return _PendingInterest(kind, segment_id, sequence)

        return None

    # end method _next_ready_interest

    # No drain event exists: the command count resets whenever the buffer is
    # observed empty (documented approximation).
    def _has_room(self, handle: ConnectionHandle) -> bool:
        if handle.buffered_amount == 0:
            self._pending_commands = 0

        return self._pending_commands < MAXIMUM_PENDING_COMMANDS

    # end method _has_room

    def _record_sent_interest(self, kind: InterestKind, segment_id: str) -> None:
        now = self._delegates.clock()

        while (
            self._recent_interests
            and now - self._recent_interests[0].sent_at > RATE_LIMIT_SUSPECT_WINDOW_MS
        ):
            self._recent_interests.popleft()

        self._recent_interests.append(_SentInterest(kind, segment_id, now))

    # end method _record_sent_interest

    def _record_sent_publish(self, publish: _QueuedPublish) -> None:
        now = self._delegates.clock()

        while self._recent_publishes and (
            len(self._recent_publishes) >= MAXIMUM_PENDING_COMMANDS
            or now - self._recent_publishes[0].sent_at > RATE_LIMIT_SUSPECT_WINDOW_MS
        ):
            self._recent_publishes.popleft()

        self._recent_publishes.append(_SentPublish(publish, now))

    # end method _record_sent_publish

    def _schedule_drain(self) -> None:
        if self._drain_timer is not None:
            return

        self._drain_timer = self._delegates.timers.call_later(
            DRAIN_RETRY_MS, self._end_drain_wait
        )

    # end method _schedule_drain

    def _end_drain_wait(self) -> None:
        self._drain_timer = None
        self._drain()

    # end method _end_drain_wait


# end class CommandQueue
