# ydbconsumer

<p align="left">
    <img src="./gopher_logo.png" width=30% alt="Ydbconsumer">
</p>

**ydbconsumer** is a simple Go application for reading messages from a [YDB](https://ydb.tech/) Topic.  
It connects over GRPCS, reads batches of messages, prints them, and commits their offsets.

---

## Features

- GRPCS connection to YDB
- Batch reading from YDB Topic
- Reading from a specific offset (`-start_offset`)
- Message metadata and body printed to stdout
- Automatic commit of received messages
- Graceful shutdown on Ctrl+C / SIGTERM
- Configurable via flags

---

## Build

```bash
go build -o ydbconsumer main.go
```

## ⚙️ Usage

```bash
./ydbconsumer \
  -endpoint "localhost:2135" \
  -database "/local" \
  -user "root" \
  -password "password" \
  -ca_file "./ydb_certs/ca.crt" \
  -topic "/my/topic" \
  -consumer "debug-consumer" \
  -batch_size 100 \
  -read_timeout 5s \
  -start_offset 0
```

By default the reader starts from the consumer's committed offset (only new, uncommitted messages).
Set `-start_offset` to re-read from a specific offset; the value is applied to every partition of the topic
(offsets are tracked per partition in YDB). A negative value restores the default behavior.