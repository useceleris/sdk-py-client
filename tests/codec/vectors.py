import json
from typing import Any, NamedTuple

from useceleris_client._messages import (
    ArrayFrame,
    ErrorFrame,
    IgnoredFrame,
    MessageFrame,
    NoticeFrame,
    PresenceConnection,
    PresenceListFrame,
    PresenceNotifyFrame,
    ServerMessage,
)

# Vector revision 2: realtime@cfa901fa73b2bd26abcc46c2f5ce47879dd4dc75 plus the
# C14 error frame and presence request ids. Hand-authored from the Rust
# layouts, as in the reference SDK; no SDK encoder creates expectations.


class EncodingVector(NamedTuple):
    name: str
    command: dict[str, Any]
    expected: bytes


class DecodingVector(NamedTuple):
    name: str
    data: bytes
    expected: ServerMessage


ENCODING_VECTORS = [
    EncodingVector(
        "publish null ID",
        {"command": "PUB", "segment_id": "default", "payload": b"hello"},
        b"@PUB\n$7\ndefault\n$-1\n$5\nhello\n",
    ),
    EncodingVector(
        "publish binary and Unicode",
        {
            "command": "PUB",
            "segment_id": "c:é",
            "message_id": "識",
            "payload": bytes([0, 255, 13, 10, 64]),
        },
        "@PUB\n$4\nc:é\n$3\n識\n$5\n".encode() + bytes([0, 255, 13, 10, 64, 10]),
    ),
    EncodingVector(
        "publish empty",
        {"command": "PUB", "segment_id": "a", "payload": b""},
        b"@PUB\n$1\na\n$-1\n$0\n\n",
    ),
    EncodingVector(
        "subscribe", {"command": "SUB", "segment_id": "chat"}, b"@SUB\n$4\nchat\n"
    ),
    EncodingVector(
        "unsubscribe",
        {"command": "UNSUB", "segment_id": "chat"},
        b"@UNSUB\n$4\nchat\n",
    ),
    EncodingVector(
        "presence subscribe",
        {"command": "PRES_SUB", "segment_id": "chat"},
        b"@PRES_SUB\n$4\nchat\n",
    ),
    EncodingVector(
        "presence unsubscribe",
        {"command": "PRES_UNSUB", "segment_id": "chat"},
        b"@PRES_UNSUB\n$4\nchat\n",
    ),
    EncodingVector(
        "presence first page",
        {
            "command": "PRES_LIST",
            "segment_id": "chat",
            "page": 1,
            "per_page": 1,
            "request_id": "1",
        },
        b"@PRES_LIST\n$4\nchat\n;1\n;1\n$1\n1\n",
    ),
    EncodingVector(
        "presence last allowed page",
        {
            "command": "PRES_LIST",
            "segment_id": "chat",
            "page": 2147483647,
            "per_page": 100,
            "request_id": "識-9",
        },
        "@PRES_LIST\n$4\nchat\n;2147483647\n;100\n$5\n識-9\n".encode(),
    ),
]

# Valid Unicode is preserved without normalization. A Python str holds an
# astral character as one code point, so the reference SDK's valid surrogate
# pairs are these code points here.
ENCODING_VECTORS += [
    EncodingVector(
        f"preserves Unicode identifier {json.dumps(identifier)}",
        {"command": "SUB", "segment_id": identifier},
        f"@SUB\n${byte_length}\n{identifier}\n".encode(),
    )
    for identifier, byte_length in [
        ("😀", 4),
        ("\U00010000", 4),
        ("\U0010ffff", 4),
        ("\ufffd", 3),
        ("e\u0301", 3),
        ("é", 2),
        ("a\0b", 3),
    ]
]

DECODING_VECTORS = [
    DecodingVector(
        "peer null ID and zero timestamp",
        b"@MSG\n+user\n+chat\n$-1\n:0\n$0\n\n",
        MessageFrame("user", "chat", None, 0, b""),
    ),
    DecodingVector(
        "peer Unicode and max timestamp",
        "@MSG\n+用戶\n+c:é\n+識\n:9223372036854775807\n$5\n".encode()
        + bytes([0, 255, 13, 10, 64, 10]),
        MessageFrame(
            "用戶", "c:é", "識", 9223372036854775807, bytes([0, 255, 13, 10, 64])
        ),
    ),
    DecodingVector(
        "CRLF and min timestamp",
        b"@SERVER_MSG\r\n:-9223372036854775808\r\n$2\r\nok\r\n",
        NoticeFrame(-9223372036854775808, b"ok"),
    ),
    DecodingVector(
        "raw notice", b"@SERVER_MSG\n:1\n$6\njoined\n", NoticeFrame(1, b"joined")
    ),
    DecodingVector(
        "simple payload and bulk identifiers",
        b"@MSG\n$1\nu\n$1\ns\n$1\nm\n:-1\n+data\n",
        MessageFrame("u", "s", "m", -1, b"data"),
    ),
    DecodingVector(
        "preserve BOM in text",
        "@MSG\n+\ufeffu\n+s\n$-1\n:1\n$0\n\n".encode(),
        MessageFrame("\ufeffu", "s", None, 1, b""),
    ),
    DecodingVector("empty array", b"*0\n", ArrayFrame(())),
    DecodingVector(
        "nested arrays",
        b"*2\n*0\n*1\n@SERVER_MSG\n:1\n$0\n\n",
        ArrayFrame((ArrayFrame(()), ArrayFrame((NoticeFrame(1, b""),)))),
    ),
    DecodingVector(
        "presence connections",
        b"@PRES_LIST_RESPONSE\n+chat\n$1\n7\n;1\n;25\n;1\n;1\n;1\n"
        b"*1\n*3\n+user\n+connection\n:123\n",
        PresenceListFrame(
            "chat",
            "7",
            1,
            25,
            1,
            1,
            1,
            (PresenceConnection("user", "connection", 123),),
        ),
    ),
    DecodingVector(
        "presence empty",
        b"@PRES_LIST_RESPONSE\n+chat\n$1\n8\n;0\n;25\n;1\n;0\n;0\n*0\n",
        PresenceListFrame("chat", "8", 0, 25, 1, 0, 0, ()),
    ),
    DecodingVector(
        "presence notification join",
        b"@PRES_NOTIFY\n+chat\n+user\n+connection\n;1\n:123\n",
        PresenceNotifyFrame("chat", "user", "connection", True, 123),
    ),
    DecodingVector(
        "presence notification leave",
        b"@PRES_NOTIFY\n+chat\n+user\n+connection\n;0\n:124\n",
        PresenceNotifyFrame("chat", "user", "connection", False, 124),
    ),
    # DECODE-01: a command this version does not know is skipped, not
    # rejected, so a newer server cannot break a deployed client.
    DecodingVector(
        "unknown command ignored", b"@FUTURE_COMMAND\n+a\n:1\n", IgnoredFrame()
    ),
    DecodingVector(
        "unknown command ignored in tail position",
        b"*2\n@SERVER_MSG\n:1\n$0\n\n@FUTURE_COMMAND\n+a\n",
        ArrayFrame((NoticeFrame(1, b""), IgnoredFrame())),
    ),
    # NODE_* commands are internal between server nodes. The SDK recognises
    # none of them, so one that arrives is skipped like any unknown command.
    DecodingVector(
        "internal node command ignored",
        b"@NODE_PUB\n+node-1\n$4\nbody\n",
        IgnoredFrame(),
    ),
    DecodingVector(
        "internal node command ignored in tail position",
        b"*2\n@SERVER_MSG\n:1\n$0\n\n@NODE_PUB\n+node-1\n",
        ArrayFrame((NoticeFrame(1, b""), IgnoredFrame())),
    ),
    DecodingVector("any NODE_ command ignored", b"@NODE_FUTURE\n+a\n", IgnoredFrame()),
    DecodingVector(
        "presence past last page",
        b"@PRES_LIST_RESPONSE\n+chat\n$1\n9\n;1\n;25\n;2\n;26\n;1\n*0\n",
        PresenceListFrame("chat", "9", 1, 25, 2, 26, 1, ()),
    ),
    DecodingVector(
        "error without sub type or resource",
        b"-Err\n+RateLimitError\n$-1\n$4\nslow\n$-1\n",
        ErrorFrame("RateLimitError", None, b"slow", None),
    ),
    # The message is length-prefixed, so it may contain anything, including
    # text that looks like another error.
    DecodingVector(
        "error message containing frame text",
        b"-Err\n+PermissionDeniedError\n+SUB\n$14\ntext\n-Err\nmore\n$4\nroom\n",
        ErrorFrame("PermissionDeniedError", "SUB", b"text\n-Err\nmore", "room"),
    ),
    DecodingVector(
        "presence query error carrying its request id",
        b"-Err\n+InternalError\n+PRES_LIST\n$27\nError getting presence data\n$1\n3\n",
        ErrorFrame("InternalError", "PRES_LIST", b"Error getting presence data", "3"),
    ),
    DecodingVector(
        "error resource of every shape",
        b"-Err\n+FutureError\n+PUB\n$1\nx\n*5\n+a\n:-5\n;7\n$-1\n*1\n$1\nb\n",
        ErrorFrame("FutureError", "PUB", b"x", ("a", -5, 7, None, ("b",))),
    ),
    # Errors are self-delimiting, so they may sit anywhere in a batch, and two
    # batched errors decode as two.
    DecodingVector(
        "batched errors before other messages",
        b"*3\n-Err\n+PermissionDeniedError\n+PRES_SUB\n$2\nno\n$4\nroom\n"
        b"-Err\n+PermissionDeniedError\n+PRES_LIST\n$2\nno\n$1\n4\n"
        b"@SERVER_MSG\n:1\n$2\nok\n",
        ArrayFrame(
            (
                ErrorFrame("PermissionDeniedError", "PRES_SUB", b"no", "room"),
                ErrorFrame("PermissionDeniedError", "PRES_LIST", b"no", "4"),
                NoticeFrame(1, b"ok"),
            )
        ),
    ),
]

MALFORMED_VECTORS = [
    b"",
    b"*0\ntrailing",
    b"*1\n",
    b"*-1\n",
    b"*4096\n",
    b"*9223372036854775808\n",
    # An unknown command is skippable only when it runs to the end of the
    # transport message; anywhere else its boundary is unknowable (DECODE-01).
    b"*2\n@FUTURE_COMMAND\n+a\n@SERVER_MSG\n:1\n$0\n\n",
    b"*2\n*1\n@FUTURE_COMMAND\n+a\n@SERVER_MSG\n:1\n$0\n\n",
    b"*2\n@NODE_PUB\n+node-1\n@SERVER_MSG\n:1\n$0\n\n",
    b"+hello\n",
    b":1\n",
    b"$-1\n",
    b"@SERVER_MSG\n:+1\n$0\n\n",
    b"@SERVER_MSG\n: 1\n$0\n\n",
    b"@SERVER_MSG\n:1.0\n$0\n\n",
    b"@SERVER_MSG\n:9223372036854775808\n$0\n\n",
    b"@SERVER_MSG\n:-9223372036854775809\n$0\n\n",
    b"@SERVER_MSG\n:1\n$-2\n",
    b"@SERVER_MSG\n:1\n$-1\n",
    b"@SERVER_MSG\n:1\n$9999999999999999999\n",
    b"@SERVER_MSG\n:1\n$2\nx\n",
    b"@SERVER_MSG\n:1\n$1\nx!",
    b"@SERVER_MSG\n:1\n$0\n\r!",
    b"@MSG\n+\n+s\n$-1\n:1\n$0\n\n",
    b"@MSG\n+u\n+s\n+\n:1\n$0\n\n",
    b"@MSG\n+u\rX\n+s\n$-1\n:1\n$0\n\n",
    b"@MSG\n$3\nu\ns\n+s\n$-1\n:1\n$0\n\n",
    # A line-based layout without field markers.
    b"-Err\nParserError\nmessage",
    b"-Other\n+ParserError\n$-1\n$1\nm\n$-1\n",
    # Missing fields.
    b"-Err\n+ParserError\n$-1\n$1\nm\n",
    b"-Err\n+ParserError\n$-1\n",
    # The type must be a simple-string name.
    b"-Err\n$11\nParserError\n$-1\n$1\nm\n$-1\n",
    b"-Err\n+Bad\rName\n$-1\n$1\nm\n$-1\n",
    b"-Err\n+Bad-Name\n$-1\n$1\nm\n$-1\n",
    b"-Err\n+" + b"ParserError" * 6 + b"\n$-1\n$1\nm\n$-1\n",
    # The sub type must be a name or null.
    b"-Err\n+ParserError\n+PRES LIST\n$1\nm\n$-1\n",
    b"-Err\n+ParserError\n$3\nSUB\n$1\nm\n$-1\n",
    b"-Err\n+ParserError\n:1\n$1\nm\n$-1\n",
    # The message cannot be null.
    b"-Err\n+ParserError\n$-1\n$-1\n$-1\n",
    # The resource must be a known fragment, within the depth limit.
    b"-Err\n+ParserError\n$-1\n$1\nm\n@SERVER_MSG\n",
    b"-Err\n+ParserError\n$-1\n$1\nm\n" + b"*1\n" * 40 + b"$-1\n",
    b"@PRES_LIST_RESPONSE\n+s\n$1\n1\n;0\n;1\n;1\n;0\n;0\n*1\n*2\n+u\n+c\n",
    # Presence figures and the join/leave flag are Integer32: signed 32-bit,
    # decimal digits only, with the `;` marker.
    *(
        b"@PRES_LIST_RESPONSE\n+s\n$1\n1\n" + total + b";1\n;1\n;0\n;0\n*0\n"
        for total in [
            b";2147483648\n",
            b";-2147483649\n",
            b";000000000001\n",
            b";+1\n",
            b"; 1\n",
            b";1.0\n",
            b":1\n",
        ]
    ),
    b"@PRES_NOTIFY\n+s\n+u\n+c\n:1\n:123\n",
    b"@MSG\n+\xff\n+s\n$-1\n:1\n$0\n\n",
]

# Ill-formed text must not collapse onto the valid replacement character.
INVALID_IDENTIFIER_VECTORS = [
    "\ud800",
    "\udfff",
    "room-\ud800",
    "\udc00-room",
    "a\ud800b",
    "\udc00\ud800",
    "\ud800\ud800",
    "😀\udfff",
    # A Python str never pairs surrogates, so the reference SDK's valid pair
    # is two unpaired code points here.
    chr(0xD800) + chr(0xDC00),
]

# Invalid text encodings remain valid opaque binary payloads.
INVALID_UTF8_VECTORS = [
    bytes([0x80]),
    bytes([0xC0, 0xAF]),
    bytes([0xED, 0xA0, 0x80]),
    bytes([0xF0, 0x9F, 0x98]),
    bytes([0xF4, 0x90, 0x80, 0x80]),
]

for invalid in INVALID_UTF8_VECTORS:
    MALFORMED_VECTORS.append(
        f"@MSG\n${len(invalid)}\n".encode() + invalid + b"\n+s\n$-1\n:1\n$0\n\n"
    )
    DECODING_VECTORS.append(
        DecodingVector(
            f"opaque non-UTF8 payload {','.join(str(byte) for byte in invalid)}",
            f"@SERVER_MSG\n:1\n${len(invalid)}\n".encode() + invalid + b"\n",
            NoticeFrame(1, invalid),
        )
    )
