# netty io_uring — vanished HTTP responses on reused connections

Minimal, self-contained reproducer: over the netty **io_uring** transport, a small fraction of
HTTP/1.1 responses on **reused (keep-alive) pooled** connections never complete and time out.
The identical load over **epoll** never drops a single response.

## Environment

| | |
|---|---|
| netty | `4.2.15.Final` (stable io_uring transport, `netty-transport-native-io_uring`) |
| reactor-netty | `1.3.6` |
| JDK | 25 (Eclipse Temurin); also reproduces on 21 |
| OS | Linux (io_uring). Verified in `maven:3-eclipse-temurin-25` on an aarch64 host |
| protocol | HTTPS/1.1, chunked streaming response, keep-alive, small connection pool |

## What it does

- Starts an embedded JDK `HttpsServer` (self-signed) that waits a short **time-to-first-byte**
  and then **streams** a chunked response. No external network needed.
- Fires a load through a pooled **reactor-netty `HttpClient`** — small pool so connections are
  **reused** — first pinned to **epoll**, then to **io_uring**, via
  `HttpClient.runOn(EventLoopGroup)`.
- Counts responses that never arrive (surface as `io.netty.handler.timeout.ReadTimeoutException`).
- Exits non-zero when io_uring drops responses that epoll did not.

Why TLS + a first-byte delay: the bug lives in io_uring's read re-arm path when `autoRead` is
disabled (reactor-netty drives reads manually) and a read is requested during
`channelReadComplete` — which `SslHandler` does constantly to complete a TLS record. The
first-byte delay recreates the window where the reused connection's socket is **not yet readable**,
so io_uring must arm `POLLIN` for the response — and occasionally never delivers it.

## Run

Requires Linux with io_uring available.

```bash
mvn -q compile exec:java
```

Under Docker, io_uring syscalls need an unrestricted seccomp profile:

```bash
docker run --rm --security-opt seccomp=unconfined \
  -v "$PWD":/proj -w /proj \
  maven:3-eclipse-temurin-25 \
  mvn -q compile exec:java
```

Runs ~3–4 minutes (12,800 requests per transport; loopback needs volume because the race hits
at ~0.1%).

### Faster / stronger repro against a real network

A real network path reproduces at ~1–3% instead of ~0.1%. Point at any streaming HTTPS endpoint:

```bash
mvn -q compile exec:java -Drepro.url=https://postman-echo.com/stream/10 \
  -Drepro.concurrency=4 -Drepro.requestsPerConn=200
```

### Tunables (`-D`)

`repro.concurrency` (pool size / parallel reused connections), `repro.requestsPerConn`,
`repro.chunks`, `repro.chunkDelayMs`, `repro.firstByteDelayMs`, `repro.sleepMs`,
`repro.readTimeoutMs`, `repro.url`.

## Expected output

```
=== epoll SUMMARY:    ok=12800 VANISHED=0 otherErr=0 (total=12800) ===
=== io_uring SUMMARY: ok=12791 VANISHED=9 otherErr=0 (total=12800) ===
=================== RESULT ===================
epoll    vanished responses: 0
io_uring vanished responses: 9
=============================================
REPRODUCED: io_uring lost responses that epoll did not.
```

epoll: **0** every run. io_uring: a handful, every run.

## Analysis / related upstream issues

Same failure class — an io_uring read that fails to re-arm `POLLIN` after `autoRead` toggling on
an already-active/reused channel, freezing the channel until the read times out — has been patched
before but appears to have a residual/adjacent race in 4.2.15 (the latest release):

- netty **#14989** *"Fix timeout when auto read is disabled late in io_uring"* (fixed 4.2.0):
  "a read call during the next `channelReadComplete` would be ignored, leading to the channel
  freezing up and the connection timing out."
- netty **#15510** / PR **#15541** *"IoUring: Correctly handle `setAutoRead(false)`"* (fixed 4.2.4),
  filed by the reactor-netty maintainer as the upstream of reactor-netty **#3833**.
- reactor-netty **#4253** (open) — native-transport stalls in 1.3.x.

## Mitigation

epoll is unaffected. Force off io_uring: `-Dreactor.netty.native=false` (→ NIO), or keep native
speed by excluding `netty-transport-native-io_uring` from the classpath so reactor-netty falls
back to epoll.
