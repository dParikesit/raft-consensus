# Raft Consensus

> This is a mirror repo based on my course project with final fixes

## Description


## Requirement

- Python

## Library

- HTTP
- socket
- xmlrpc
- json
- time
- asyncio
- threading
- sys

## How to Run

#### Leader Server

```
Format: python3 server.py [ip] [port]
```

Example

```
python3 server.py localhost 3000
```

#### Server

```
Format: python3 server.py [ip] [port] [ip_leader] [port_leader]
```

Example

```
python3 server.py localhost 3001 localhost 3000
```

#### Client

```
Format: python3 client.py [ip] [port] [client_ID]
```

Example

```
python3 client.py localhost 3000 client01
```

## Accepted client command:
| Command | Description |
| ---------- | ------ |
| `enqueue(x)` | x is a string. This command is used to add to the queue. Will return queue length after enqueued |
| `dequeue` | This command is used to pop the queue. Will return the popped value |
| `log` | This command is used to return the log recorded by leader |
| `-1  ` | Exit the client |