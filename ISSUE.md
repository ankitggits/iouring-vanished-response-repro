# io_uring: HTTP/1.1 responses intermittently never complete on reused TLS keep-alive connections (read never re-armed); epoll unaffected

## Summary

With the **io_uring** transport, a small fraction of HTTP/1.1 responses on **reused (keep-alive)
pooled** connections never complete: no response bytes are delivered to the pipeline and the
channel stalls until `io.netty.handler.timeout.ReadTimeoutException` fires. The **epoll** transport
under byte-identical load never drops a single response. The failure only ever affects the **2nd or
later request on a reused connection**, which points at the read re-arm path when `autoRead` is
toggled on an already-active channel. It reproduces even with a **single, serially-reused
connection** (so it is not a multi-connection or ring-contention race), and TLS makes it far easier
to trigger.

## Environment

| | |
|---|---|
| netty | **4.2.15.Final** (latest release), stable io_uring transport (`netty-transport-native-io_uring`, `netty-transport-classes-io_uring`) |
| reactor-netty | 1.3.6 (`reactor-netty-http` / `reactor-netty-core`) |
| JVM | Eclipse Temurin 25 (also reproduces on 21) |
| OS | Linux with io_uring. Verified inside `maven:3-eclipse-temurin-25` (Ubuntu) on an aarch64 host, `--security-opt seccomp=unconfined` |
| protocol | HTTPS/1.1, chunked streaming response, keep-alive, small connection pool (connections reused) |

## Expected behavior

Every response on a pooled, reused connection completes — exactly as with epoll under the same
load.

## Actual behavior

A fraction of responses on reused connections never arrive; the channel freezes until
`ReadTimeoutHandler` fires. Rate observed:

| transport | loopback (embedded server) | real network (`postman-echo.com/stream/10`, HTTPS) |
|---|---|---|
| epoll | 0 vanished | 0 vanished |
| io_uring | ~0.1% (needs volume) | ~1–3% |

Representative run (defaults: concurrency=16, 800 requests/connection = 12,800 per transport):

```
=== epoll SUMMARY:    ok=12800 VANISHED=0  otherErr=0 (total=12800) ===
=== io_uring SUMMARY: ok=12768 VANISHED=32 otherErr=0 (total=12800) ===
epoll    vanished responses: 0
io_uring vanished responses: 32
```

The vanished responses surface as:

```
io.netty.handler.timeout.ReadTimeoutException
```

## Steps to reproduce

A minimal, self-contained project accompanies this report (embedded self-signed JDK `HttpsServer`
that streams a chunked response after a short time-to-first-byte; a pooled reactor-netty
`HttpClient` with a small pool so connections are reused; the same load is run once pinned to epoll
and once to io_uring via `HttpClient.runOn(EventLoopGroup)`). No external network is required.

```bash
# Linux with io_uring. Under Docker, io_uring syscalls require an unrestricted seccomp profile.
docker run --rm --security-opt seccomp=unconfined -v "$PWD":/proj -w /proj \
  maven:3-eclipse-temurin-25 mvn -q compile exec:java
```

Runtime ~3–4 min (loopback needs volume, since the race hits at ~0.1%). For a faster/stronger
signal point at a real streaming HTTPS endpoint:

```bash
mvn -q compile exec:java -Drepro.url=https://postman-echo.com/stream/10 \
  -Drepro.concurrency=4 -Drepro.requestsPerConn=200
```

## Evidence / isolation

| Test | Result | Conclusion |
|---|---|---|
| epoll vs io_uring, identical config | epoll **0**, io_uring drops every run | io_uring-specific, not config/server/timeout |
| `concurrency=1` (single serially-reused connection) | still drops (e.g. `ok=298 VANISHED=2` / 300) | not a multi-connection / ring-contention race; per-connection |
| first request vs subsequent | only 2nd+ request on a connection is lost | read re-arm on an **already-active reused** channel |
| plaintext HTTP loopback | could not reproduce | TLS-driven read pattern greatly increases the hit rate |
| HTTPS + time-to-first-byte delay | reproduces | needs a window where the reused socket is not-yet-readable so `POLLIN` must be (re)armed |

## Root-cause analysis

reactor-netty's HTTP client runs with **`autoRead = false`** and drives reads manually for
backpressure, suspending reads when a connection is released to the pool and resuming them when it
is re-acquired for the next request. On a reused connection the response then arrives after a short
latency, i.e. the socket is not readable at the moment the read is (re)armed. Under io_uring the
`POLLIN` for that read is occasionally never delivered, so the response — although present on the
socket — is never read, and the channel freezes until the read timeout.

TLS amplifies this because `SslHandler` requests additional reads during `channelReadComplete` to
complete a record, repeatedly exercising the same re-arm path.

This is the same failure class that has been patched before, which suggests a residual/adjacent race
still present in 4.2.15:

- **#14989** — "Fix timeout when auto read is disabled late in io_uring" (fixed 4.2.0):
  *"If auto read is disabled late (during a channelRead), but there is a read call during the next
  channelReadComplete, that read call would be ignored, leading to the channel freezing up and the
  connection timing out."* Fix: *"When clearRead cancels the scheduled POLLIN, clear the iostate
  immediately, so that the next read() will correctly schedule a new POLLIN."*
- **#15510 / #15541** — "IoUring: Correctly handle `channel.config().setAutoRead(false)`"
  (fixed 4.2.4), filed by the reactor-netty maintainer as the upstream of reactor-netty **#3833**:
  *"We did not correctly handle cancellations when changing auto-read state in an already active
  channel … could lead to stalls."*

Both fixes predate 4.2.15, so what remains is either a further edge in the same POLLIN re-arm logic
or an interaction with per-request auto-read toggling that these fixes did not cover.

Related open reports: reactor-netty **#4253** (native-transport stalls in 1.3.x).

## Suggested area to investigate

The io_uring auto-read / `POLLIN` (re-)arm path when `setAutoRead(false)` is toggled on an
already-active channel and a read is requested while the socket is not yet readable — i.e. the same
code touched by #14989 and #15541 — for a case where the scheduled read does not result in a
delivered `POLLIN` completion on a reused connection.

## Workaround

epoll is unaffected. Force off io_uring via `-Dreactor.netty.native=false` (falls back to NIO), or
keep native performance by excluding `netty-transport-native-io_uring` from the classpath so
reactor-netty selects epoll.
