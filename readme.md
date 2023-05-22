# Tugas Besar 1 IF3230 - Sistem Paralel dan Terdistribusi - Consensus Protocol: Raft

Implementasi Protokol Konsensus Protokol

## Requirement

- Python

## Library

- HTTP
- socket

## How to Run

#### Leader Server

```
Format: python3 server.py [ip] [port]
```

Contoh

```
python3 server.py localhost 3000
```

#### Server

```
Format: python3 server.py [ip] [port] [ip_leader] [port_leader]
```

Contoh

```
python3 server.py localhost 3001 localhost 3000
```

#### Client

```
Format: python3 client.py [ip] [port] [client_ID]
```

Contoh

```
python3 client.py localhost 3000 client01
```

Command client yang diterima:
| Command | Keterangan |
| ---------- | :------: |
| `enqueue(x)` | x merupakan integer. Command ini digunakan untuk melakukan eksekusi enqueue pada aplikasi |
| `dequeue` | Command ini digunakan untuk melakukan eksekusi dequeue pada aplikasi |
| `log` | Command ini digunakan untuk eksekusi request_log yang akan mengembalikan log yang dimiliki Leader |
| `-1  ` | Command ini digunakan untuk mengakhiri command oleh client |

## Anggota

| Nama                       |   NIM    |
| -------------------------- | :------: |
| Muhammad Zubair            | 13519172 |
| Marcellus Michael Herman K | 13520057 |
| Adelline Kania Setiyawan   | 13520084 |
| Dimas Shidqi Parikesit     | 13520087 |
| Monica Adelia              | 13520096 |
