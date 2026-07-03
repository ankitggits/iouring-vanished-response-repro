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

/**
 * Reproduces a reactor-netty "vanished response" over the netty <b>io_uring</b> transport:
 * a small fraction of HTTPS/1.1 responses on reused (keep-alive) pooled connections never
 * complete and time out, while the identical load over <b>epoll</b> never drops any.
 *
 * <p>The load target is an embedded JDK {@link HttpsServer} (self-signed TLS) that waits a
 * short "time to first byte" and then streams a chunked response, so there is no external
 * dependency and the whole run is offline. The client is a pooled reactor-netty
 * {@link HttpClient} — which drives reads with {@code autoRead=false} and toggles read per
 * request on pooled connections, and whose {@code SslHandler} requests further reads during
 * {@code channelReadComplete} — pinned to an explicit io_uring / epoll event loop via
 * {@link HttpClient#runOn(EventLoopGroup)}.
 *
 * <p>Linux only (io_uring / epoll). Exits non-zero if io_uring dropped any response.
 *
 * <p>Tune with -D flags: repro.concurrency, repro.requestsPerConn, repro.chunks,
 * repro.chunkDelayMs, repro.firstByteDelayMs, repro.sleepMs, repro.readTimeoutMs.
 */
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

    /**
     * Entry point.
     *
     * @param args ignored
     * @throws Exception on startup/load failure
     */
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

    /**
     * Embedded HTTPS/1.1 keep-alive server (self-signed) that waits {@link #FIRST_BYTE_DELAY_MS}
     * then streams {@link #CHUNKS} chunks with a small delay between them (chunked
     * transfer-encoding), mimicking a streaming upstream reached over the network.
     */
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
