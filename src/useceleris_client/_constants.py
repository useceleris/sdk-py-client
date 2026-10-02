# Every fixed value the package uses, in one place. Internal: none of these is
# part of the public surface.

# Outgoing size (LIMIT-01). Size is only ever checked on commands the client
# sends. Received messages are already fully buffered when they arrive, so
# checking them bounds no memory and only discards data.
#
# The command bound is the server's transport ceiling: above it no plan can
# accept a command. Each plan's own, smaller payload cap is enforced by the
# server and surfaces as a MessageSizeLimitError server error.
MAXIMUM_COMMAND_BYTES = 2 * 1024 * 1024

# Equal to the command bound, so a maximum-size command always fits an empty
# buffer and anything queued behind it reports backpressure instead.
MAXIMUM_BUFFERED_BYTES = MAXIMUM_COMMAND_BYTES

MAXIMUM_PENDING_COMMANDS = 64

# Outbound recovery (RESEND-01). A rate limit is reported without saying which
# frame it dropped, so whatever went out recently is resent.
#
# The server reports drops at most once a second, and a second more covers the
# round trip, so a report concerns only commands sent within this window.
RATE_LIMIT_SUSPECT_WINDOW_MS = 2_000

# Resending waits at least this long, past the per-second window the dropped
# frames were counted in.
RATE_LIMIT_COOLDOWN_MS = 1_000

# Bounds the extra load, and the extra usage, a rate limit can cause.
MAXIMUM_PUBLISH_RESENDS = 1

# After this many rate limits in a row the limit is treated as a used-up quota
# (per hour or per month): recent publishes are no longer resent, and recent
# subscriptions wait for a quota probe.
MAXIMUM_CONSECUTIVE_RATE_LIMITS = 8

# A used-up quota refuses every frame, so the subscriptions it dropped are
# re-sent rarely rather than abandoned: first after a minute, then doubling.
QUOTA_PROBE_FIRST_DELAY_MS = 60_000

QUOTA_PROBE_MAXIMUM_DELAY_MS = 3_600_000

# How often a full writer is checked again: the socket has no drain event.
DRAIN_RETRY_MS = 50

# The error type the server sends for any rate limit.
RATE_LIMIT_ERROR_TYPE = "RateLimitError"

# Generated message ids share every receiver's dedup window with ids from other
# publishers, so they are random and long enough never to collide.
MESSAGE_ID_RANDOM_BYTES = 16

# Decoder bounds. These limit parsing work and recursion, not message size; no
# legitimate server message approaches them.
MAXIMUM_FRAGMENTS = 4096

MAXIMUM_DEPTH = 32

# "PRES_LIST_RESPONSE", the longest command the server sends.
MAXIMUM_COMMAND_NAME_BYTES = 18

MAXIMUM_ERROR_NAME_BYTES = 64

# Integer64 (`:`) carries timestamps; it is also the range of bulk and array
# lengths. "-9223372036854775808" is its widest value.
MAXIMUM_INTEGER64_LINE_BYTES = 20

MINIMUM_INTEGER64 = -(1 << 63)

MAXIMUM_INTEGER64 = (1 << 63) - 1

# Integer32 (`;`) carries every other integer. "-2147483648" is its widest
# value.
MAXIMUM_INTEGER32_LINE_BYTES = 11

MINIMUM_INTEGER32 = -2_147_483_648

MAXIMUM_INTEGER32 = 2_147_483_647

# Connection defaults. Consumers do not configure where Celeris lives;
# overriding the base URL is for local stacks and other deployments
# (ENDPOINT-01).
DEFAULT_BASE_URL = "wss://realtime.useceleris.com"

DEFAULT_CONNECT_TIMEOUT_MS = 15_000

DEFAULT_PRESENCE_QUERY_TIMEOUT_MS = 10_000

DEFAULT_SEGMENT_ID = "default"

# The command a presence query error names as its sub type (QUERY-01).
PRESENCE_LIST_COMMAND = "PRES_LIST"

MAXIMUM_PRESENCE_PAGE_SIZE = 100

MAXIMUM_CHANNEL_REFERENCE_LENGTH = 255

# Recovery.
MAXIMUM_RETRIES = 10

RETRY_BUDGET_RESET_MS = 60_000

RETRY_BASE_DELAY_MS = 500

RETRY_DELAY_CAP_MS = 30_000

# A rate limit already in hand disproves recovery only if commands flowed
# unrefused for longer than the longest pause plus the report window; any
# sooner, it may be a late report of the frames that just went out.
QUOTA_RETURN_CONFIRMATION_MS = RETRY_DELAY_CAP_MS + RATE_LIMIT_SUSPECT_WINDOW_MS

REPLAY_OVERLAP_MS = 5_000

# The server's largest replay lookback: an unsigned 32-bit millisecond count.
REPLAY_LOOKBACK_CAP_MS = 4_294_967_295

CLOSE_BUDGET_MS = 5_000

DEDUP_WINDOW_SIZE = 1024
