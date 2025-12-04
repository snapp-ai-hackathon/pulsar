import asyncio
from nats.aio.client import Client as NatsClient


async def subscribe(address, subject):
    nc = NatsClient()

    await nc.connect(address)

    async def message_handler(msg):
        print(f"Received on [{msg.subject}]: {msg.data.decode()}")

    # Subscribe to your topic
    await nc.subscribe(subject=subject, cb=message_handler)

    print("Listening on {0}...".format(subject))
    while True:
        await asyncio.sleep(1)
