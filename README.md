# Debezium CDC Demo with Postgres, Redis Streams, and FastStream

This project demonstrates Change Data Capture (CDC) using Debezium with PostgreSQL as the source database and Redis Streams as the sink, consumed by a FastStream application.

## Architecture

- **PostgreSQL**: Source database with a `users` table
- **Debezium Server**: Captures database changes and writes to Kafka
- **Kafka**: Message broker that receives CDC events
- **Kafka UI**: Web interface for managing and monitoring Kafka clusters (available at http://localhost:8080)
- **Kafka-to-Redis Bridge**: Simple Python script that moves events from Kafka to Redis Streams
- **Redis**: Stores CDC events in Redis Streams
- **FastStream**: Modern async Python framework for consuming Redis Stream events

## Prerequisites

- Docker and Docker Compose
- Python 3.11+

## Setup and Run

### 1. Start all services

```bash
docker-compose up -d
```

Wait about 10-15 seconds for all services to start. The FastStream consumer will automatically start listening for CDC events.

### 2. Install Python dependencies (for running main.py locally)

```bash
pip install -r requirements.txt
```

### 3. Run the demo script

```bash
python main.py
```

This will:
- Insert new users (Charlie, Diana)
- Update a user's email
- Delete a user

### 4. Watch CDC events in real-time

View the FastStream consumer logs to see CDC events as they happen:

```bash
docker-compose logs -f faststream-consumer
```

You should see formatted output showing CREATE, UPDATE, and DELETE operations.

### 5. View CDC events in Redis (optional)

Check Redis Streams directly:

```bash
# View events from the stream
docker exec -it redis redis-cli XREAD COUNT 10 STREAMS dbserver1.public.users 0

# Check stream length
docker exec -it redis redis-cli XLEN dbserver1.public.users

# View all events
docker exec -it redis redis-cli XRANGE dbserver1.public.users - +
```

## View Logs

```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f faststream-consumer
docker-compose logs -f debezium
docker-compose logs -f postgres
```

## Cleanup

```bash
docker-compose down -v
rm -rf debezium-data
```

## What's Happening?

1. Python script (`main.py`) writes to PostgreSQL
2. Debezium Server captures changes via PostgreSQL's logical replication
3. Debezium writes CDC events to Kafka (`dbserver1.public.users`)
4. Kafka-to-Redis bridge (`kafka_to_redis.py`) moves events from Kafka to Redis Streams
5. FastStream consumer (`consumer.py`) subscribes to the Redis Stream and processes events
6. Events are displayed in real-time with formatted output showing the operation type and data

## Project Files

- `docker-compose.yml` - Orchestrates Postgres, Redis, Kafka, Kafka UI, Debezium Server, and FastStream consumer
- `init.sql` - Creates the `users` table and initial data
- `main.py` - Demo script that performs database operations
- `consumer.py` - FastStream application that consumes and displays CDC events
- `requirements.txt` - Python dependencies

## Why FastStream?
It's new and simple.

FastStream also provides:
- Modern async/await support
- Built-in Redis Streams support
- Easy-to-use decorators for message handling
- Type hints and validation
- Production-ready error handling
