# Tugas Besar 1 IF3230 - Sistem Paralel dan Terdistribusi - Consensus Protocol: Raft

Implementasi Protokol Konsensus Protokol

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

## Desain Sistem dan Strategi

1. Komunikasi menggunakan XMLRPC
2. Pesan menggunakan string json yang dikirimkan lewat RPC
3. Implementasi Client

- Client akan menghubungi address dari server yang berasal dari inputan user. Jika node tidak aktif, client akan gagal terhubung. Jika node yang dihubungi bukan leader, node akan mengembalikan response dengan status "Redirect" beserta address dari node leader. Jika pada saat itu tidak ada leader sama sekali, node akan mengembalikan response dengan status "No Leader", dan program client akan berhenti.
- Request yang dikirim ke server untuk dieksekusi memiliki clientID, requestNumber, dan command. Hal ini dilakukan untuk memastikan bahwa setiap requestNumber minimal telah di terima oleh server dan maksimal dieksekusi sekali.
- Koneksi dengan server diberi batasan timeout untuk melakukan retry pengiriman request.

4. Implementasi Membership Change
5. Implementasi Leader Election
6. Implementasi Log Replication
7. Implementasi Execute

- Ketika fungsi execute dipanggil, fungsi ini akan menerima parameter json_request. Fungsi ini akan melakukan pengecekan terlebih dahulu apakah node merupakan leader atau bukan. Jika bukan, dia akan mengirimkan response redirect dengan isi address leader. Jika tidak ada leader, node akan memberikan response no leader pada client. Fungsi akan berhenti.
- Jika node yang dihubungi adalah node leader, node akan membuat response success. Lalu menambahkannya pada logEntry. Setelah itu, akan dilakukan log replication. Baru terakhir, akan dikembalikan json response.

8. Implementasi log request
9. Implementasi Heartbeat

## Anggota

| Nama                       |   NIM    |
| -------------------------- | :------: |
| Muhammad Zubair            | 13519172 |
| Marcellus Michael Herman K | 13520057 |
| Adelline Kania Setiyawan   | 13520084 |
| Dimas Shidqi Parikesit     | 13520087 |
| Monica Adelia              | 13520096 |
