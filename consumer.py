import os
import asyncio
import json
from faststream import FastStream
from faststream.redis import RedisBroker

# executed from console python consumer.py

# Get Redis URL from environment or default to localhost
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Create Redis broker
broker = RedisBroker(REDIS_URL)

# Create FastStream app
app = FastStream(broker)


@broker.subscriber(stream="dbserver1.public.users")
async def handle_user_changes(msg: dict):
    """
    Handle CDC events from Debezium via Redis Streams.
    """
    try:
        # The bridge (kafka_to_redis.py) may send a simplified message or full payload
        # Depending on how it's configured. Let's handle both.
        
        if "data" in msg:
            # Simplified message from bridge
            payload = json.loads(msg["data"])
        else:
            # Full Debezium payload (directly in msg)
            payload = msg

        operation = payload.get("op", "unknown")

        operation_names = {
            "c": "CREATE",
            "u": "UPDATE",
            "d": "DELETE",
            "r": "READ"
        }

        op_name = operation_names.get(operation, operation)

        print(f"\n{'='*60}")
        print(f"🔔 CDC Event Received: {op_name}")
        print(f"{'='*60}")

        if operation in ["c", "u", "r"]:
            after = payload.get("after", {})
            print(f"Data: {json.dumps(after, indent=2)}")

        if operation == "u":
            before = payload.get("before", {})
            print(f"Before: {json.dumps(before, indent=2)}")
            print(f"After: {json.dumps(after, indent=2)}")

        if operation == "d":
            before = payload.get("before", {})
            print(f"Deleted: {json.dumps(before, indent=2)}")

        # Source metadata
        source = payload.get("source", {})
        print(f"\nMetadata:")
        print(f"  Table: {source.get('table')}")
        print(f"  Timestamp: {payload.get('ts_ms')}")
        print(f"{'='*60}\n")

    except Exception as e:
        print(f"❌ Error processing message: {e}")
        print(f"Raw message: {msg}")


@app.on_startup
async def on_startup():
    print("✓ FastStream consumer started")
    print("✓ Listening for CDC events on Redis Streams...")
    print("✓ Stream: dbserver1.public.users\n")


if __name__ == "__main__":
    asyncio.run(app.run())
