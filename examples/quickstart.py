"""useceleris-client quickstart.

One Channel is one WebSocket connection; every segment of the channel is
multiplexed over it. Connecting joins the "default" segment automatically.
Segment objects are lightweight proxies over the channel's connection.

Credentials: a trusted server signs short-lived opaque credentials. The inline
signer below stands in for YOUR application's credential endpoint; in
production keep the signing secret server-side (useceleris-server) and fetch
credentials from there.

Run with CELERIS_WS_URL, CELERIS_CLIENT_ID and CELERIS_SIGNING_SECRET set.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import os
import time

from useceleris_client import (
    CredentialRequest,
    Credentials,
    MessageMetadata,
    create_client,
    json_payload,
    read_text,
)


def sign_credentials() -> Credentials:
    claims = {
        "timestamp": int(time.time() * 1000),
        # Lets this single connection see its own publishes.
        "allow_echo": True,
    }
    payload = base64.b64encode(
        json.dumps(claims, separators=(",", ":")).encode()
    ).decode()
    digest = hmac.new(
        os.environ["CELERIS_SIGNING_SECRET"].encode(), payload.encode(), hashlib.sha512
    ).hexdigest()
    signature = base64.b64encode(
        f"{os.environ['CELERIS_CLIENT_ID']}:{digest}".encode()
    ).decode()

    return Credentials(payload=payload, signature=signature)


# end function sign_credentials


# Called once per connection attempt, so credentials are always fresh.
async def provide_credentials(request: CredentialRequest) -> Credentials:
    return sign_credentials()


# end function provide_credentials


async def main() -> None:
    client = create_client(
        base_url=os.environ["CELERIS_WS_URL"],
        # A local ws:// stack; production uses wss://.
        allow_insecure_loopback=True,
        credential_provider=provide_credentials,
    )
    channel = client.channel(f"quickstart-{int(time.time() * 1000)}")
    channel.events().on_state_change(lambda state: print("state:", state))
    channel.events().on_error(lambda error: print("error:", error.code))

    await channel.connect()

    # Subscribe, publish, receive: on one connection, thanks to allow_echo.
    chat = channel.segment("chat")
    delivered: list[str] = []
    membership = chat.subscribe()

    def receive(payload: bytes, metadata: MessageMetadata) -> None:
        # Payload first; metadata carries the sender, the id and the time.
        delivered.append(read_text(payload))

    # end function receive

    chat.on_message(receive)
    await asyncio.sleep(1)

    await chat.publish(json_payload({"hello": "world"}))
    # Returning means the local socket accepted the bytes, never a receipt.

    for _ in range(60):
        if delivered:
            break

        await asyncio.sleep(0.25)

    # Presence: who is in the segment right now (one query in flight per
    # channel).
    page = await chat.presence_list(page=1, per_page=10)

    membership.cancel()
    await channel.close()

    print(f"example: ok delivered={len(delivered)} present={page.total}")


# end function main


if __name__ == "__main__":
    asyncio.run(main())
