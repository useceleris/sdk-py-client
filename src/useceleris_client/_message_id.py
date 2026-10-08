import re
import secrets

from useceleris_client._constants import (
    MESSAGE_ID_RANDOM_BYTES,
    SDK_LANGUAGE,
    SDK_VERSION,
)

# An id holds only letters, digits, dots and underscores.
_OTHER_VERSION_CHARACTER = re.compile("[^A-Za-z0-9.]")


# Every publish carries an id, so a resent copy is recognisable and receivers
# drop it (RESEND-01). A generated id names the SDK that made it, then holds 16
# random bytes as hex: msg__py_v1.0.0__0123456789abcdef0123456789abcdef. The
# double underscores keep it apart from the server's own msg_{node}_ ids.
def generate_message_id() -> str:
    return message_id_prefix(SDK_LANGUAGE, SDK_VERSION) + secrets.token_hex(
        MESSAGE_ID_RANDOM_BYTES
    )


# end function generate_message_id


# "msg__", the language, "_v", the version, "__". Any other character of the
# version, such as the hyphen of a pre-release, becomes an underscore.
def message_id_prefix(language: str, version: str) -> str:
    return f"msg__{language}_v{_OTHER_VERSION_CHARACTER.sub('_', version)}__"


# end function message_id_prefix
