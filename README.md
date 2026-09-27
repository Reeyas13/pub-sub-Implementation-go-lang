# Go WebSocket Pub/Sub

A lightweight in-memory Pub/Sub implementation in Go using Gorilla WebSocket.

## Features

- WebSocket client management
- Topic-based subscriptions
- Publish messages to subscribers
- Unsubscribe support
- Concurrent-safe connections
- Automatic client cleanup

## Example

Subscribe to a topic:

```js
ws.send(JSON.stringify({
  action: "subscribe",
  topic: "test"
}))
```

Publish a message:

```js
ws.send(JSON.stringify({
  action: "publish",
  topic: "test",
  message: {
    text: "Hello world"
  }
}))
```

## Run

```bash
go run .
```

Server runs on:

```text
http://localhost:3000
```

## Tech

- Go
- Gorilla WebSocket
- UUID

## Note

This project is a simple Pub/Sub implementation built for learning and experimentation. It currently stores clients and subscriptions in memory.