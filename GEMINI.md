# XPulse - Realtime Notification System

XPulse is a high-performance, realtime notification system built as a trusted plugin for the `xcore` ecosystem. It leverages Redis Pub/Sub for message distribution and Server-Sent Events (SSE) to deliver messages to clients.

## Project Overview

- **Purpose**: Provide a scalable multi-channel notification stream for users.
- **Main Technologies**:
  - **Python 3.10+**: Core logic.
  - **FastAPI**: Provides the HTTP layer and SSE streaming capabilities via `StreamingResponse`.
  - **Redis (asyncio)**: Used as the Pub/Sub backbone for message distribution.
  - **xcore SDK**: Integration layer for lifecycle management and inter-plugin communication.

## Architecture

- **Entry Point**: `src/main.py` defines the `Plugin` class, which handles:
  - Plugin lifecycle (`on_load`, `on_unload`).
  - Event handlers for internal `xcore` events (e.g., `ext.notification.publish`, `ext.notification.broadcast`).
  - HTTP routes for client streaming and manual publishing.
- **Client Layer**:
  - `RedisPubSubManager` (`src/client/redis_client.py`): Manages the Redis connection pool, subscriptions, and the SSE generator logic.
  - `RedisConfiguration` (`src/client/section.py`): Typed configuration handler that parses environment variables.
- **Concurrency**: Fully asynchronous, using `asyncio` for handling multiple concurrent SSE streams and Redis operations.

## Key Features

- **Multi-channel SSE**: A single connection can subscribe to multiple channels.
- **Multi-tenant Filtering**: Messages are filtered by `user_id` to ensure users only receive notifications intended for them.
- **Heartbeat System**: Built-in SSE "ping" to keep connections alive through proxies and load balancers.
- **Auto-reconnection**: Robust Redis reconnection logic with exponential backoff.
- **Internal Actions**: Provides the `xpulse.stream` action for other plugins to trigger notifications.

## Building and Running

### Prerequisites
- Python 3.10+
- Redis Server
- `xcore` kernel/SDK

### Configuration
The plugin is configured via environment variables defined in `plugin.yaml` or the environment:
- `url`: Redis connection URL (e.g., `redis://localhost:6379/0`).
- `channel`: List of default channels to listen to.
- `MAX_CONCURRENT_STREAMS`: Maximum number of simultaneous SSE connections.
- `MAX_CHANNELS_PER_STREAM`: Maximum channels a single client can subscribe to.

### Running
As a plugin, it is typically started by the `xcore` kernel:
```bash
# Example (exact command depends on xcore kernel CLI)
xcore plugin load .
```

## Development Conventions

- **Type Hinting**: Extensive use of Python type hints for clarity and safety.
- **Logging**: Uses standard library `logging`, with specific loggers for core logic and Redis operations.
- **Error Handling**: Custom exceptions (`InvalidChannel`, `StreamLimitExceeded`) are translated to appropriate HTTP status codes (400, 503).
- **Clean Abstraction**: Redis logic is isolated from the FastAPI/Plugin logic in the `client` package.

## API Usage Example (JS)

```javascript
const user_id = "user123";
const channels = "notification,system_alerts";
const src = new EventSource(`/stream/${user_id}?channels=${channels}`);

src.addEventListener('notification', (e) => {
    const data = JSON.parse(e.data);
    console.log("New notification:", data.text);
});

src.addEventListener('error', (e) => {
    console.error("SSE Connection failed", e);
});
```
