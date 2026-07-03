<!--
Suggested issue title (paste into the GitHub title field):
io_uring: HTTP/1.1 responses intermittently never complete on reused TLS keep-alive connections (read never re-armed); epoll unaffected

Everything below this comment is the issue body — copy/paste it as-is.
-->

With the **io_uring** transport, a small fraction of HTTP/1.1 responses on **reused (keep-alive)
pooled** connections never complete: no response bytes are delivered to the pipeline and the
channel stalls until `io.netty.handler.timeout.ReadTimeoutException` fires. The **epoll** transport
under byte-identical load never drops a single response.

The failure only ever affects the **2nd or later request on a reused connection** (never the
first), it reproduces with a **single, serially-reused connection** (so it is not a multi-connection
or ring-contention race), and TLS makes it far easier to trigger. This looks like a
residual/adjacent case of the auto-read / `POLLIN` re-arm family fixed earlier in #14989 and
#15510/#15541 (details under *Root cause analysis*).

### Expected behavior

Every response on a pooled, reused connection completes — exactly as it does with the epoll
transport under identical load.

### Actual behavior

A fraction of responses on reused connections never arrive; the channel freezes until
`ReadTimeoutHandler` fires with:

```
io.netty.handler.timeout.ReadTimeoutException
```

Observed rates (same code, only the transport changes):

| transport | loopback (embedded server) | real network (`postman-echo.com/stream/10`, HTTPS) |
|---|---|---|
| epoll | 0 vanished | 0 vanished |
| io_uring | ~0.1% (needs volume) | ~1–3% |

Isolation:

| Test | Result | Conclusion |
|---|---|---|
| epoll vs io_uring, identical config | epoll **0**, io_uring drops every run | io_uring-specific |
| `concurrency=1` (single serially-reused connection) | still drops (e.g. `ok=298 VANISHED=2` / 300) | per-connection; not a ring/pool race |
| first request vs subsequent | only the 2nd+ request on a connection is lost | read re-arm on an already-active reused channel |
| plaintext HTTP loopback | not reproduced | TLS read pattern greatly increases the hit rate |
| HTTPS + short time-to-first-byte | reproduced | needs a window where the reused socket is not-yet-readable so `POLLIN` must be armed |

### Steps to reproduce

Self-contained (no external network): an embedded self-signed JDK `HttpsServer` streams a chunked
response after a short time-to-first-byte; a pooled reactor-netty `HttpClient` with a small pool (so
connections are reused) runs the same load once pinned to epoll and once to io_uring via
`HttpClient.runOn(EventLoopGroup)`. Full code below. Drop it into a Maven project and run:

```bash
# Linux with io_uring. Under Docker, io_uring syscalls need an unrestricted seccomp profile.
docker run --rm --security-opt seccomp=unconfined -v "$PWD":/proj -w /proj \
  maven:3-eclipse-temurin-25 mvn -q compile exec:java
```

Representative output (defaults: concurrency=16, 800 requests/connection = 12,800 per transport):

```
=== epoll SUMMARY:    ok=12800 VANISHED=0  otherErr=0 (total=12800) ===
=== io_uring SUMMARY: ok=12768 VANISHED=32 otherErr=0 (total=12800) ===
epoll    vanished responses: 0
io_uring vanished responses: 32
```

epoll is 0 every run; io_uring drops a handful every run. A real network reproduces faster/stronger:
`-Drepro.url=https://postman-echo.com/stream/10 -Drepro.concurrency=4 -Drepro.requestsPerConn=200`.

### Minimal yet complete reproducer code

`pom.xml` — dependencies and run plugin (`maven.compiler.release=21`):

```xml
<dependencies>
  <dependency>
    <groupId>io.projectreactor.netty</groupId>
    <artifactId>reactor-netty-http</artifactId>
    <version>1.3.6</version>
  </dependency>
  <dependency>
    <groupId>io.netty</groupId>
    <artifactId>netty-all</artifactId>
    <version>4.2.15.Final</version>
  </dependency>
</dependencies>
<!-- build > plugins: org.codehaus.mojo:exec-maven-plugin (3.5.0) with
     <mainClass>io.github.iouringrepro.VanishedResponseRepro</mainClass> -->
```

`src/main/java/io/github/iouringrepro/VanishedResponseRepro.java`:

```java
package io.github.iouringrepro;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.timeout.ReadTimeoutHandler;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public final class VanishedResponseRepro {

    private static final int CONNECT_TIMEOUT_MS = 2_000;
    private static final int MAX_IDLE_TIME_MS   = 72_000;
    private static final int MAX_LIFE_TIME_MS   = 600_000;

    private static final int CONCURRENCY         = Integer.getInteger("repro.concurrency", 16);
    private static final int REQUESTS_PER_CONN   = Integer.getInteger("repro.requestsPerConn", 800);
    private static final int CHUNKS              = Integer.getInteger("repro.chunks", 10);
    private static final int CHUNK_DELAY_MS      = Integer.getInteger("repro.chunkDelayMs", 8);
    // Time-to-first-byte: the server waits before the first response byte, recreating the
    // network/upstream latency window in which the reused connection's socket is not yet
    // readable — this is where io_uring fails to re-arm POLLIN and the response vanishes.
    private static final int FIRST_BYTE_DELAY_MS = Integer.getInteger("repro.firstByteDelayMs", 40);
    private static final int SLEEP_MS            = Integer.getInteger("repro.sleepMs", 2);
    private static final int READ_TIMEOUT_MS     = Integer.getInteger("repro.readTimeoutMs", 1_000);

    // Optional: hit an external streaming HTTPS endpoint instead of the embedded server. A real
    // network path reproduces at a much higher rate (~1-3%) than loopback (~0.1%, needs volume).
    private static final String URL = System.getProperty("repro.url", "");

    private VanishedResponseRepro() {
    }

    public static void main(String[] args) throws Exception {
        SslContext clientSsl = SslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .build();

        HttpsServer server = URL.isBlank() ? startStreamingServer(new SelfSignedCertificate()) : null;
        String url = server != null
            ? "https://127.0.0.1:" + server.getAddress().getPort() + "/stream"
            : URL;
        if (server != null) {
            System.out.printf("embedded TLS server: %s  (firstByteDelay=%dms, chunks=%d, chunkDelay=%dms)%n",
                url, FIRST_BYTE_DELAY_MS, CHUNKS, CHUNK_DELAY_MS);
        } else {
            System.out.printf("external endpoint: %s%n", url);
        }
        System.out.printf("load: concurrency=%d, requestsPerConn=%d (total=%d per transport), readTimeout=%dms%n",
            CONCURRENCY, REQUESTS_PER_CONN, CONCURRENCY * REQUESTS_PER_CONN, READ_TIMEOUT_MS);

        try {
            int epollHang   = runLoad("epoll", url, clientSsl);
            int ioUringHang = runLoad("io_uring", url, clientSsl);

            System.out.println();
            System.out.println("=================== RESULT ===================");
            System.out.printf("epoll    vanished responses: %d%n", epollHang);
            System.out.printf("io_uring vanished responses: %d%n", ioUringHang);
            System.out.println("=============================================");

            if (ioUringHang > 0 && epollHang == 0) {
                System.out.println("REPRODUCED: io_uring lost responses that epoll did not.");
                System.exit(1);
            }
            if (ioUringHang == 0) {
                System.out.println("Not reproduced this run (io_uring clean) — increase repro.requestsPerConn and retry.");
            }
        } finally {
            if (server != null) {
                server.stop(0);
            }
        }
    }

    private static int runLoad(String transport, String url, SslContext clientSsl) throws Exception {
        int workers = Math.max(2, Runtime.getRuntime().availableProcessors());
        EventLoopGroup group = "io_uring".equals(transport)
            ? new MultiThreadIoEventLoopGroup(workers, IoUringIoHandler.newFactory())
            : new MultiThreadIoEventLoopGroup(workers, EpollIoHandler.newFactory());

        ConnectionProvider provider = ConnectionProvider.builder("repro")
            .maxConnections(CONCURRENCY)                       // small pool -> connections are reused
            .maxIdleTime(Duration.ofMillis(MAX_IDLE_TIME_MS))
            .maxLifeTime(Duration.ofMillis(MAX_LIFE_TIME_MS))
            .pendingAcquireMaxCount(CONCURRENCY * 4)
            .build();

        HttpClient client = HttpClient.create(provider)
            .runOn(group)                                      // pin the transport under test
            .secure(spec -> spec.sslContext(clientSsl))
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MS)
            .doOnConnected(conn -> conn.addHandlerLast(
                new ReadTimeoutHandler(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS)));

        AtomicInteger ok = new AtomicInteger();
        AtomicInteger hang = new AtomicInteger();
        AtomicInteger err = new AtomicInteger();
        int total = CONCURRENCY * REQUESTS_PER_CONN;
        System.out.printf("%n=== %s: firing %d requests over %d reused connections ===%n", transport, total, CONCURRENCY);

        ExecutorService pool = Executors.newFixedThreadPool(CONCURRENCY);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int c = 0; c < CONCURRENCY; c++) {
                futures.add(pool.submit(() -> {
                    for (int i = 0; i < REQUESTS_PER_CONN; i++) {
                        runOne(client, url, ok, hang, err);
                        sleep(SLEEP_MS);
                    }
                }));
            }
            for (Future<?> f : futures) {
                f.get();
            }
        } finally {
            pool.shutdownNow();
            provider.disposeLater().block(Duration.ofSeconds(10));
            group.shutdownGracefully().sync();
        }

        System.out.printf("=== %s SUMMARY: ok=%d VANISHED=%d otherErr=%d (total=%d) ===%n",
            transport, ok.get(), hang.get(), err.get(), total);
        return hang.get();
    }

    private static void runOne(HttpClient client, String url,
                               AtomicInteger ok, AtomicInteger hang, AtomicInteger err) {
        try {
            client.get()
                .uri(url)
                .responseContent()
                .aggregate()
                .asByteArray()
                .block(Duration.ofMillis(READ_TIMEOUT_MS * 4L));   // let the netty ReadTimeout surface first
            ok.incrementAndGet();
        } catch (Exception e) {
            if (isReadTimeout(e)) {
                hang.incrementAndGet();
                System.out.printf("VANISHED response <-- %s%n", e.getClass().getSimpleName());
            } else {
                err.incrementAndGet();
                System.out.printf("ERROR %s: %s%n", e.getClass().getSimpleName(), e.getMessage());
            }
        }
    }

    /** A vanished response surfaces as a read timeout somewhere in the cause chain. */
    private static boolean isReadTimeout(Throwable e) {
        return Stream.iterate(e, Objects::nonNull, Throwable::getCause).anyMatch(t ->
            t instanceof io.netty.handler.timeout.TimeoutException
            || t instanceof java.util.concurrent.TimeoutException
            || (t instanceof IllegalStateException
                && String.valueOf(t.getMessage()).startsWith("Timeout on blocking read")));
    }

    private static HttpsServer startStreamingServer(SelfSignedCertificate cert) throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("key", cert.key(), new char[0], new Certificate[] {cert.cert()});
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, new char[0]);
        SSLContext ssl = SSLContext.getInstance("TLS");
        ssl.init(kmf.getKeyManagers(), null, null);

        HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(ssl));
        server.setExecutor(Executors.newFixedThreadPool(Math.max(8, CONCURRENCY * 2)));
        server.createContext("/stream", exchange -> {
            sleep(FIRST_BYTE_DELAY_MS);                          // simulate upstream/network latency (TTFB)
            exchange.getResponseHeaders().add("Content-Type", "text/plain");
            exchange.sendResponseHeaders(200, 0);               // 0 => chunked, arbitrary length
            try (OutputStream os = exchange.getResponseBody()) {
                for (int i = 0; i < CHUNKS; i++) {
                    os.write(("chunk-" + i + "\n").getBytes(StandardCharsets.UTF_8));
                    os.flush();
                    sleep(CHUNK_DELAY_MS);
                }
            } catch (Exception ignored) {
                // client went away; irrelevant to the repro
            } finally {
                exchange.close();
            }
        });
        server.start();
        return server;
    }

    private static void sleep(int ms) {
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
```

### Netty version

`4.2.15.Final` (latest release), stable io_uring transport — `netty-transport-native-io_uring` /
`netty-transport-classes-io_uring`. Client is reactor-netty `1.3.6`.

### JVM version (java -version)

```
openjdk version "25.0.3" 2026-04-21 LTS
OpenJDK Runtime Environment Temurin-25.0.3+9 (build 25.0.3+9-LTS)
OpenJDK 64-Bit Server VM Temurin-25.0.3+9 (build 25.0.3+9-LTS, mixed mode, sharing)
```

Also reproduces on JDK 21.

### OS version (uname -a)

```
Linux 6.8.0-100-generic #100-Ubuntu SMP PREEMPT_DYNAMIC aarch64 GNU/Linux
```

Verified inside the `maven:3-eclipse-temurin-25` (Ubuntu) container with
`--security-opt seccomp=unconfined`. The defect is not believed to be architecture-specific.

### Root cause analysis

reactor-netty's HTTP client runs with **`autoRead = false`** and drives reads manually for
backpressure, suspending reads when a connection is released to the pool and resuming them when it
is re-acquired for the next request. On a reused connection the response then arrives after a short
latency, i.e. the socket is not readable at the moment the read is (re)armed. Under io_uring the
`POLLIN` for that read is occasionally never delivered, so the response — although present on the
socket — is never read and the channel freezes until the read timeout. TLS amplifies this because
`SslHandler` requests additional reads during `channelReadComplete` to complete a record, repeatedly
exercising the same re-arm path.

This is the same failure class patched before, which suggests a residual/adjacent race in 4.2.15:

- **#14989** — "Fix timeout when auto read is disabled late in io_uring" (fixed 4.2.0): *"a read
  call during the next channelReadComplete would be ignored, leading to the channel freezing up and
  the connection timing out."* Fix: *"When clearRead cancels the scheduled POLLIN, clear the iostate
  immediately, so that the next read() will correctly schedule a new POLLIN."*
- **#15510 / #15541** — "IoUring: Correctly handle `channel.config().setAutoRead(false)`" (fixed
  4.2.4), filed by the reactor-netty maintainer as the upstream of reactor-netty #3833.

Both fixes predate 4.2.15, so what remains is either a further edge in the same `POLLIN` re-arm
logic or an interaction with per-request auto-read toggling that they did not cover. Likely area to
investigate: the io_uring auto-read / `POLLIN` (re-)arm path when `setAutoRead(false)` is toggled on
an already-active channel and a read is requested while the socket is not yet readable.

**Workaround:** epoll is unaffected — force off io_uring via `-Dreactor.netty.native=false` (falls
back to NIO), or exclude `netty-transport-native-io_uring` from the classpath so reactor-netty
selects epoll.
