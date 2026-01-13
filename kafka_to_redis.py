import os
import json
import time
from kafka import KafkaConsumer
import redis

# this is executed from the docker compose

def main():
    # Get configuration from environment variables or use defaults for local development
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")

    print(f"Waiting for Kafka ({kafka_bootstrap_servers}) to be ready...")
    time.sleep(20)

    print(f"Connecting to Redis at {redis_host}:{redis_port}...")
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    print(f"Connecting to Kafka at {kafka_bootstrap_servers}...")
    consumer = KafkaConsumer(
        'dbserver1.public.users',
        bootstrap_servers=kafka_bootstrap_servers,
        auto_offset_reset='earliest',
        group_id='redis-sink'
    )

    print("✓ Kafka to Redis bridge started. Listening for CDC events...")

    for message in consumer:
        try:
            if message.value is None:
                continue
            
            event = json.loads(message.value.decode('utf-8'))

            # Create a simplified event for Redis
            stream_data = {
                'operation': event.get('payload', {}).get('op', 'unknown'),
                'timestamp': str(event.get('payload', {}).get('ts_ms', '')),
                'data': json.dumps(event.get('payload', {}))
            }

            # Add to Redis Stream
            stream_key = f"dbserver1.public.users"
            r.xadd(stream_key, stream_data)

            op = stream_data['operation']
            print(f"✓ Forwarded {op} event to Redis stream: {stream_key}")

        except Exception as e:
            print(f"Error processing message: {e}")

if __name__ == "__main__":
    main()
