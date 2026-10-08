import secrets

from useceleris_client._constants import MESSAGE_ID_RANDOM_BYTES


# Every publish carries an id, so a resent copy is recognisable and receivers
# drop it (RESEND-01).
def generate_message_id() -> str:
    return secrets.token_hex(MESSAGE_ID_RANDOM_BYTES)


# end function generate_message_id
