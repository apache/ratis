/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.examples.counter.client;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.examples.common.Constants;
import org.apache.ratis.examples.counter.CounterCommand;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.quic.QuicConfigKeys;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.security.TlsConf;
import org.apache.ratis.security.TlsConf.CertificatesConf;
import org.apache.ratis.security.TlsConf.PrivateKeyConf;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * RaftBench &mdash; throughput/latency benchmark for the two Raft transports.
 *
 * <p>Clients are split into two roles that run concurrently:
 * <ul>
 *   <li><b>writers</b> send payload writes to the leader (Raft routes them there);</li>
 *   <li><b>readers</b> do plain stale reads ({@code minIndex=0}) either from the leader
 *       or, round-robin, from the followers. Because a reader does not read its own
 *       write, no read-your-writes catch-up is needed &mdash; reads stay fast on both
 *       transports.</li>
 * </ul>
 * Each swept total client count is divided into readers/writers by {@code --read-ratio}.
 * Write and read latency/throughput are measured separately.
 *
 * <pre>
 * Usage:
 *   RaftBench --transport {TCP_TLS|QUIC}
 *             --clients    FROM:TO:STEP        total clients (e.g. 5:30:5)
 *             --read-ratio R                   fraction that are readers (default 0.7)
 *             --read-from  {leader|followers}  where readers read (default followers)
 *             --payload    SIZE                (e.g. 64, 1kB, 1MB)
 *             --requests   N                   per client
 *             --conn       {A|B}               A = new connection per request, B = reused
 *             [--warmup    W]                  warmup iterations per client (default 20)
 *             [--csv       FILE]               append results
 *             [--run-id    S]                  tags every CSV row with this sweep id (default "-")
 *             [--rep       N]                  repetition number of this measurement (default 1)
 *             [--lat-file  F]                  dump raw samples: "w &lt;ns&gt;" / "r &lt;ns&gt;" /
 *                                              "hop &lt;rtt&gt; &lt;c2l&gt; &lt;l2c&gt;" (scal.sh pools them
 *                                              across client nodes; the files are kept)
 *             [--worker-offset K]              first worker id of this process (default 0);
 *                                              set when several RaftBench processes share one
 *                                              cluster so their ids (= state-machine keys) do
 *                                              not collide, e.g. 0 / 8 / 16 / 24 for 4 x 8
 * </pre>
 */
public final class RaftBench {

  enum Transport { TCP_TLS, QUIC }

  /** A = brand-new connection per request; B = one reused connection per client. */
  enum ConnMode { A, B }

  /** Where readers read from. */
  enum ReadFrom { LEADER, FOLLOWERS }

  /** scaling (obsolete, kept for comparison) = writer/reader split, plain stale read (minIndex=0);
   *  rywrites = each worker writes to the leader, then reads its OWN key from its assigned
   *  follower as a plain stale read (minIndex=0, served immediately - no long-poll, no waiting).
   *  --read-ratio and --read-from apply only to scaling; rywrites ignores both. */
  enum Mode { SCALING, RYWRITES }

  private static Transport transport = Transport.TCP_TLS;

  /** Identity of a single measurement, so a CSV row stays unique even after files are merged:
   *  runId groups one sweep, rep numbers the repetitions of an otherwise identical config
   *  (methodology: discard rep 1 as the cold JVM, take the median of the rest). */
  private static String runId = "-";
  private static int rep = 1;

  // ---- Client construction ------------------------------------------------

  static RaftClient newClient() {
    final RaftProperties properties = new RaftProperties();
    final Parameters parameters = new Parameters();

    if (transport == Transport.QUIC) {
      RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.QUIC);
      QuicConfigKeys.Client.setTlsCaCert(properties, "ratis-test/src/test/resources/ssl/ca.crt");
      RaftClientConfigKeys.Rpc.setRequestTimeout(properties,
          TimeDuration.valueOf(5_000, TimeUnit.MILLISECONDS));
    } else {
      RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.NETTY);
      RaftClientConfigKeys.Rpc.setRequestTimeout(properties,
          TimeDuration.valueOf(5_000, TimeUnit.MILLISECONDS));
      final TlsConf tlsConf = new TlsConf.Builder()
          .setName("client")
          .setPrivateKey(new PrivateKeyConf(new File("ratis-test/src/test/resources/ssl/client.pem")))
          .setKeyCertificates(new CertificatesConf(new File("ratis-test/src/test/resources/ssl/client.crt")))
          .setTrustCertificates(new CertificatesConf(new File("ratis-test/src/test/resources/ssl/ca.crt")))
          .setMutualTls(false)
          .build();
      NettyConfigKeys.Client.setTlsConf(parameters, tlsConf);
    }

    return RaftClient.newBuilder()
        .setProperties(properties)
        .setParameters(parameters)
        .setRaftGroup(Constants.RAFT_GROUP)
        // Domyslne retryForeverNoSleep ponawia zadanie do martwego wezla W NIESKONCZONOSC -
        // worker wisi i licznik requests_failed nigdy nie tyka. Dlatego limit, ale HOJNY:
        // 100 prob x 100 ms = do 10 s na zadanie. Wczesniejsze 10 prob (~1 s) bylo za ciasne -
        // przy 5 serwerach i 25-30 klientach startujacych naraz odkrycie lidera potrafi zjesc
        // kilkanascie NotLeaderException (klient trafia na losowego peera, lider jest zajety),
        // proces RaftBench wywalal sie na starcie i punkt konczyl sie jako 4/5 wezlow.
        .setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(100,
            TimeDuration.valueOf(100, TimeUnit.MILLISECONDS)))
        .build();
  }

  private static final DateTimeFormatter WALL_CLOCK = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

  /** Wall-clock stamp for the progress lines below. Latencies are measured with nanoTime;
   *  this is only so a stuck run can be lined up against the server logs and the script output. */
  private static String now() {
    return LocalTime.now().format(WALL_CLOCK);
  }

  /** Write payload = "INCREMENT" + clientId(4B) + payloadSize(4B) + zero-bytes. */
  static Message buildWrite(int clientId, int payloadSize) {
    final int headerLen = 8;
    final ByteBuffer header = ByteBuffer.allocate(headerLen);
    header.putInt(clientId);
    header.putInt(payloadSize);
    header.flip();
    final ByteString body = ByteString.copyFrom(header)
        .concat(ByteString.copyFrom(new byte[Math.max(0, payloadSize - headerLen)]));
    return Message.valueOf(CounterCommand.INCREMENT.getMessage().getContent().concat(body));
  }

  /** Read request = "GET" + workerId(4B) + payloadSize(4B). The state machine replies with the REAL
   *  bytes stored for that worker (its last write), or a zero payload of that size if none yet. */
  static Message buildRead(int workerId, int payloadSize) {
    final ByteBuffer b = ByteBuffer.allocate(2 * Integer.BYTES);
    b.putInt(workerId);
    b.putInt(payloadSize);
    b.flip();
    return Message.valueOf(
        CounterCommand.GET.getMessage().getContent().concat(ByteString.copyFrom(b)));
  }

  // ---- One (transport, total, payload, conn, read-from) data point --------

  static final class Result {
    final int total;
    final int writers;
    final int readers;
    final long durationNanos;
    final long[] writeLatNanos;
    final long[] readLatNanos;
    /** Zadania wyslane w oknie pomiaru; zatwierdzone = writeLatNanos.length (probka jest
     *  tylko z udanego zapisu), wiec failed = sent - committed z konstrukcji. */
    final long sent;
    /** Zerwane / nieudane polaczenia zaobserwowane przez klientow (odpowiednik
     *  "utraconych polaczen" z tabel 6.1-6.3). */
    final long connFailed;
    /** Probki sondy hop do LIDERA (ns): RTT oraz oba kierunki osobno (patrz splitHops). */
    final long[] hopRttNanos;
    final long[] hopC2lNanos;
    final long[] hopL2cNanos;

    /** Wariant dla trybu scaling (bez licznikow i sondy). */
    Result(int total, int writers, int readers, long durationNanos,
        long[] writeLatNanos, long[] readLatNanos) {
      this(total, writers, readers, durationNanos, writeLatNanos, readLatNanos,
          writeLatNanos.length, 0, new long[0], new long[0], new long[0]);
    }

    Result(int total, int writers, int readers, long durationNanos,
        long[] writeLatNanos, long[] readLatNanos, long sent, long connFailed,
        long[] hopRttNanos, long[] hopC2lNanos, long[] hopL2cNanos) {
      this.total = total;
      this.writers = writers;
      this.readers = readers;
      this.durationNanos = durationNanos;
      this.writeLatNanos = writeLatNanos;
      this.readLatNanos = readLatNanos;
      this.sent = sent;
      this.connFailed = connFailed;
      this.hopRttNanos = hopRttNanos;
      this.hopC2lNanos = hopC2lNanos;
      this.hopL2cNanos = hopL2cNanos;
    }
  }

  static Result runBench(int total, int writers, int readers, int payloadSize,
      int requestsPerClient, int warmup, ConnMode conn, ReadFrom readFrom,
      RaftPeerId leaderId, List<RaftPeerId> followers) throws InterruptedException {

    final List<Thread> threads = new ArrayList<>(writers + readers);
    final Map<Integer, long[]> writeLat = new ConcurrentHashMap<>();
    final Map<Integer, long[]> readLat = new ConcurrentHashMap<>();
    final CountDownLatch ready = new CountDownLatch(writers + readers);
    final CountDownLatch go = new CountDownLatch(1);

    // ---- writer threads: payload writes to the leader ----
    for (int w = 0; w < writers; w++) {
      final int id = w;
      final Message writeMsg = buildWrite(id, payloadSize);
      final Thread t = new Thread(() -> runRole(id, conn, warmup, requestsPerClient, ready, go,
          writeLat, client -> client.io().send(writeMsg)), "bench-writer-" + w);
      t.start();
      threads.add(t);
    }

    // ---- reader threads: stale reads from leader or a round-robin follower ----
    for (int r = 0; r < readers; r++) {
      final int id = r;
      final RaftPeerId target =
          (readFrom == ReadFrom.LEADER) ? leaderId : followers.get(r % followers.size());
      final Message readMsg = buildRead(id, payloadSize);
      final Thread t = new Thread(() -> runRole(id, conn, warmup, requestsPerClient, ready, go,
          readLat, client -> client.io().sendStaleRead(readMsg, 0, target)), "bench-reader-" + r);
      t.start();
      threads.add(t);
    }

    ready.await();               // all threads constructed their clients
    final long start = System.nanoTime();
    go.countDown();              // release them together
    for (Thread t : threads) {
      t.join();
    }
    final long durationNanos = System.nanoTime() - start;

    return new Result(total, writers, readers, durationNanos,
        merge(writeLat.values()), merge(readLat.values()));
  }

  /** One benchmark role (writer or reader): warmup + measured loop of {@code op}. */
  private static void runRole(int id, ConnMode conn, int warmup, int requests,
      CountDownLatch ready, CountDownLatch go, Map<Integer, long[]> latBucket, Op op) {
    final long[] lat = new long[requests];
    final RaftClient reused = (conn == ConnMode.B) ? newClient() : null;
    try {
      ready.countDown();
      go.await();
      final int total = warmup + requests;
      for (int i = 0; i < total; i++) {
        final RaftClient client = (conn == ConnMode.A) ? newClient() : reused;
        try {
          final long t0 = System.nanoTime();
          op.run(client);
          final long t1 = System.nanoTime();
          if (i >= warmup) {
            lat[i - warmup] = t1 - t0;
          }
        } finally {
          if (conn == ConnMode.A) {
            client.close();
          }
        }
      }
      latBucket.put(id, lat);
    } catch (Exception e) {
      System.err.printf("role %d failed: %s%n", id, e);
    } finally {
      if (reused != null) {
        try { reused.close(); } catch (IOException ignored) { }
      }
    }
  }

  @FunctionalInterface
  private interface Op {
    void run(RaftClient client) throws IOException;
  }

  // ---- read-your-writes mode ---------------------------------------------

  /** Each of {@code total} workers loops (blocking, request->response->next):
   *  write to the leader, then read its OWN key from its assigned follower as a plain
   *  stale read ({@code minIndex=0}, served immediately from the follower's current state).
   *  Write and read latency are measured separately.
   *
   *  <p>Worker ids run from {@code offset} to {@code offset + total - 1}. The id is the
   *  state-machine key, and every process numbers its workers from zero, so two processes
   *  on one cluster would write over each other's keys unless each gets its own offset.
   *  The follower is chosen by the global id, so the spread over followers comes out the same
   *  as if one process ran all the workers. */
  static Result runBenchRywrites(int total, int offset, int payloadSize, int requestsPerClient,
      int warmup, ConnMode conn, RaftPeerId leaderId, List<RaftPeerId> followers)
      throws InterruptedException {

    final List<Thread> threads = new ArrayList<>(total);
    final Map<Integer, long[]> writeLat = new ConcurrentHashMap<>();
    final Map<Integer, long[]> readLat = new ConcurrentHashMap<>();
    final Map<Integer, long[]> stamps = new ConcurrentHashMap<>();
    final LongAdder sent = new LongAdder();
    final LongAdder connFailed = new LongAdder();
    final CountDownLatch ready = new CountDownLatch(total);
    final CountDownLatch go = new CountDownLatch(1);

    for (int w = 0; w < total; w++) {
      final int id = offset + w;
      final RaftPeerId follower = followers.get(id % followers.size());
      final Thread t = new Thread(() -> runRywritesWorker(id, conn, warmup, requestsPerClient,
          payloadSize, follower, ready, go, writeLat, readLat, stamps, sent, connFailed),
          "bench-ryw-" + id);
      t.start();
      threads.add(t);
    }

    ready.await();
    final long wallStart = System.nanoTime();
    go.countDown();

    // Sonda hop: rownolegle z obciazeniem odpytuje LIDERA malym PING-iem (query ze znacznikami
    // nanoTime serwera). Z 4 znacznikow na probke wychodza srednie czasy klient->lider i
    // lider->klient bez synchronizacji zegarow - rachunek jak w NTP, patrz splitHops.
    final List<long[]> hopSamples = Collections.synchronizedList(new ArrayList<>());
    final AtomicBoolean hopStop = new AtomicBoolean(false);
    Thread hopThread = null;
    if (leaderId != null) {
      hopThread = new Thread(() -> runHopProbe(leaderId, hopStop, hopSamples), "bench-hop");
      hopThread.setDaemon(true);
      hopThread.start();
    }

    for (Thread t : threads) {
      t.join();
    }
    final long wallEnd = System.nanoTime();
    if (hopThread != null) {
      hopStop.set(true);
      hopThread.interrupt();
      hopThread.join(3000);
    }

    // Okno pomiaru ze znacznikow per worker: rozgrzewka zostaje POZA zegarem (wczesniej
    // siedziala w mianowniku tput i zanizala go ~17%, niesymetrycznie miedzy transportami,
    // bo w conn A rozgrzewka to 20 handshakow). Bez zadnej dodatkowej bariery miedzy
    // watkami. Fallback na czas scienny, gdyby zaden worker nie doszedl do czesci mierzonej.
    long minStart = Long.MAX_VALUE;
    long maxEnd = Long.MIN_VALUE;
    for (long[] s : stamps.values()) {
      minStart = Math.min(minStart, s[0]);
      maxEnd = Math.max(maxEnd, s[1]);
    }
    final long durationNanos = stamps.isEmpty() ? wallEnd - wallStart : maxEnd - minStart;

    final long[][] hops = splitHops(hopSamples);
    return new Result(total, total, total, durationNanos,
        merge(writeLat.values()), merge(readLat.values()),
        sent.sum(), connFailed.sum(), hops[0], hops[1], hops[2]);
  }

  /** One read-your-writes worker: write to leader -> N -> read own write from {@code follower}.
   *
   *  <p>Kazde zadanie ma WLASNY try/catch i worker po bledzie leci dalej, a probki i liczniki
   *  publikuje w finally. Bez tego padniety worker znikal z wynikow RAZEM ze swoimi udanymi
   *  pomiarami (survivor bias - ogon wygladal lepiej dokladnie tam, gdzie bylo najgorzej),
   *  a kontrola "wyslane vs zatwierdzone" zawsze wychodzila zerowa. */
  private static void runRywritesWorker(int id, ConnMode conn, int warmup, int requests,
      int payloadSize, RaftPeerId follower, CountDownLatch ready, CountDownLatch go,
      Map<Integer, long[]> writeBucket, Map<Integer, long[]> readBucket,
      Map<Integer, long[]> windowStamps, LongAdder sentAdder, LongAdder connFailedAdder) {
    final long[] wlat = new long[requests];
    final long[] rlat = new long[requests];
    int wCount = 0;
    int rCount = 0;
    long sent = 0;
    long connFailed = 0;
    long windowStart = 0;
    long windowEnd = 0;
    int fails = 0;              // wszystkie bledy (takze w rozgrzewce) - tylko do linii postepu
    long lastW = 0;
    long lastR = 0;
    final Message writeMsg = buildWrite(id, payloadSize);
    final Message readMsg = buildRead(id, payloadSize);   // same worker id => reads its own write
    // Same client writes to the leader and reads its own write from the assigned follower.
    // minIndex=0: the follower serves its current value IMMEDIATELY (no waiting) => no latency tail.
    // It usually already holds this worker's latest write; occasionally it is one write behind.
    RaftClient reused = null;
    try {
      ready.countDown();   // ZAWSZE przed pierwszym wywolaniem, ktore moze rzucic - inaczej
      go.await();          // runBenchRywrites wisi na ready.await() bez timeoutu
      if (conn == ConnMode.B) {
        reused = newClient();
      }
      final int total = warmup + requests;
      final long tStart = System.nanoTime();
      System.out.printf("[%s] worker %d: START, %d zadan, follower=%s%n", now(), id, total, follower);
      System.out.flush();
      for (int i = 0; i < total; i++) {
        final boolean measured = i >= warmup;
        if (measured && windowStart == 0) {
          windowStart = System.nanoTime();   // zegar punktu liczy sie OD KONCA rozgrzewki
        }
        RaftClient client = null;
        try {
          client = (conn == ConnMode.A) ? newClient() : reused;
          if (measured) {
            sent++;
          }
          final long t0 = System.nanoTime();
          client.io().send(writeMsg);                        // -> leader (consensus)
          final long t1 = System.nanoTime();
          if (measured) {
            wlat[wCount++] = t1 - t0;
          }
          lastW = t1 - t0;
          client.io().sendStaleRead(readMsg, 0, follower);   // -> assigned follower, immediate
          final long t2 = System.nanoTime();
          if (measured) {
            rlat[rCount++] = t2 - t1;
          }
          lastR = t2 - t1;
        } catch (Exception e) {
          fails++;
          if (isConnFailure(e)) {
            connFailed++;
          }
          if (fails <= 3 || fails % 50 == 0) {
            System.err.printf("[%s] worker %d: zadanie %d nieudane (blad nr %d): %s%n",
                now(), id, i, fails, e);
          }
          if (conn == ConnMode.B) {
            // Zerwane polaczenie ubiloby tez wszystkie nastepne zadania na tym kliencie -
            // odtwarzamy klienta, jak zrobilby prawdziwy uzytkownik po utracie polaczenia.
            try {
              if (reused != null) {
                reused.close();
              }
            } catch (IOException ignored) { }
            reused = newClient();
          }
        } finally {
          if (conn == ConnMode.A && client != null) {
            try {
              client.close();
            } catch (IOException ignored) { }
          }
        }
        if (measured) {
          windowEnd = System.nanoTime();
        }
        // Postep na zywo co 10 zadan: bez tego widac dopiero koncowy CSV i nie da sie
        // odroznic "wolno" od "wisi". w/r = czas ostatniego UDANEGO zapisu i odczytu.
        if ((i + 1) % 10 == 0) {
          final double el = (System.nanoTime() - tStart) / 1e9;
          System.out.printf("[%s] worker %d: %d/%d  %.1fs  %.2f req/s  w=%.0fms r=%.0fms  bledy=%d%n",
              now(), id, i + 1, total, el, (i + 1) / el, lastW / 1e6, lastR / 1e6, fails);
          System.out.flush();
        }
      }
    } catch (Exception e) {
      System.err.printf("rywrites worker %d failed: %s%n", id, e);
    } finally {
      // Publikacja takze CZESCIOWYCH wynikow po awarii calego workera - patrz javadoc.
      writeBucket.put(id, Arrays.copyOf(wlat, wCount));
      readBucket.put(id, Arrays.copyOf(rlat, rCount));
      if (windowStart != 0) {
        windowStamps.put(id, new long[] {windowStart, windowEnd != 0 ? windowEnd : System.nanoTime()});
      }
      sentAdder.add(sent);
      connFailedAdder.add(connFailed);
      if (reused != null) {
        try { reused.close(); } catch (IOException ignored) { }
      }
    }
  }

  /** Zerwane/nieudane polaczenie - klasyfikacja po lancuchu przyczyn wyjatku.
   *  Timeout zadania NIE jest zerwaniem polaczenia (liczy sie tylko do requests_failed). */
  private static boolean isConnFailure(Throwable e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      final String cls = t.getClass().getSimpleName();
      if (cls.contains("Connect") || cls.contains("ClosedChannel") || cls.contains("Socket")
          || cls.contains("AlreadyClosed")) {
        return true;
      }
      final String m = t.getMessage();
      if (m != null) {
        final String lm = m.toLowerCase(Locale.ROOT);
        if (lm.contains("connection reset") || lm.contains("connection refused")
            || lm.contains("broken pipe") || lm.contains("connection closed")) {
          return true;
        }
      }
    }
    return false;
  }

  // ---- Hop probe: klient->lider i lider->klient bez synchronizacji zegarow ----

  /** Sonda hop: co ~100 ms maly "PING" do {@code target} sciezka query (stale read, bez
   *  konsensusu); serwer odpowiada "PONG &lt;t1&gt; &lt;t2&gt;" (nanoTime wejscia do query i budowy
   *  odpowiedzi). Probka = {t0,t1,t2,t3}: klient-wyslal, serwer-odebral, serwer-odeslal,
   *  klient-odebral. Sonda biegnie ROWNOLEGLE z obciazeniem, wiec mierzy hop pod takim
   *  obciazeniem, jakie panuje w danym punkcie. Pierwsze 3 probki (rozgrzewka polaczenia
   *  sondy) sa odrzucane; nieudana probka (elekcja, przeciazenie) po prostu przepada. */
  private static void runHopProbe(RaftPeerId target, AtomicBoolean stop, List<long[]> out) {
    try (RaftClient client = newClient()) {
      final Message ping = Message.valueOf("PING");
      int i = 0;
      while (!stop.get()) {
        try {
          final long t0 = System.nanoTime();
          final RaftClientReply reply = client.io().sendStaleRead(ping, 0, target);
          final long t3 = System.nanoTime();
          final String s = reply.getMessage().getContent().toStringUtf8();
          if (s.startsWith("PONG ")) {
            final String[] p = s.split(" ");
            if (++i > 3) {
              out.add(new long[] {t0, Long.parseLong(p[1]), Long.parseLong(p[2]), t3});
            }
          }
        } catch (Exception e) {
          // elekcja / przeciazenie / zamkniete polaczenie: probka przepada, sonda idzie dalej
        }
        Thread.sleep(100);
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    } catch (IOException ignored) {
    }
  }

  /** Rozklada probki sondy na RTT i czasy jednokierunkowe.
   *
   *  <p>nanoTime obu maszyn ma dowolne zera i lekki dryf, wiec przesuniecie zegarow theta
   *  wyznaczaja DWIE kotwice - probka o najmniejszym RTT z pierwszej i z drugiej polowy
   *  przebiegu - interpolowane liniowo w czasie (dryf pierwszego rzedu znika). Theta z jednej
   *  probki to theta prawdziwa plus polowa asymetrii drogi, dlatego kotwica jest ta
   *  o najmniejszym RTT: najblizsza symetrii, bez kolejek. To ten sam rachunek co w NTP.
   *  RTT = (t3-t0)-(t2-t1); c2l = t1-t0-theta; l2c = t3-t2+theta.
   *  Zwraca {rtt[], c2l[], l2c[]} w ns. */
  private static long[][] splitHops(List<long[]> samples) {
    final int n = samples.size();
    final long[] rtt = new long[n];
    final long[] c2l = new long[n];
    final long[] l2c = new long[n];
    if (n == 0) {
      return new long[][] {rtt, c2l, l2c};
    }
    for (int i = 0; i < n; i++) {
      final long[] s = samples.get(i);
      rtt[i] = (s[3] - s[0]) - (s[2] - s[1]);
    }
    final int a1 = argMinRtt(rtt, 0, n >= 6 ? n / 2 : n);
    final int a2 = n >= 6 ? argMinRtt(rtt, n / 2, n) : a1;
    final double th1 = theta(samples.get(a1));
    final double th2 = theta(samples.get(a2));
    final long x1 = samples.get(a1)[0];
    final long x2 = samples.get(a2)[0];
    for (int i = 0; i < n; i++) {
      final long[] s = samples.get(i);
      final double th = (x2 == x1)
          ? th1 : th1 + (th2 - th1) * ((double) (s[0] - x1)) / (x2 - x1);
      c2l[i] = Math.round(s[1] - s[0] - th);
      l2c[i] = Math.round(s[3] - s[2] + th);
    }
    return new long[][] {rtt, c2l, l2c};
  }

  private static double theta(long[] s) {
    return ((s[1] - s[0]) + (s[2] - s[3])) / 2.0;
  }

  private static int argMinRtt(long[] rtt, int from, int to) {
    int best = from;
    for (int i = from; i < to; i++) {
      if (rtt[i] < rtt[best]) {
        best = i;
      }
    }
    return best;
  }

  // ---- Stats --------------------------------------------------------------

  private static long[] merge(Iterable<long[]> arrays) {
    final List<Long> all = new ArrayList<>();
    for (long[] a : arrays) {
      for (long v : a) {
        all.add(v);
      }
    }
    final long[] out = new long[all.size()];
    for (int i = 0; i < out.length; i++) {
      out[i] = all.get(i);
    }
    return out;
  }

  /** p in [0,100]; array is sorted in place. */
  private static double percentileMs(long[] latNanos, double p) {
    if (latNanos.length == 0) {
      return 0;
    }
    Arrays.sort(latNanos);
    final int idx = (int) Math.min(latNanos.length - 1L,
        Math.round(p / 100.0 * (latNanos.length - 1)));
    return latNanos[idx] / 1_000_000.0;
  }

  private static double meanMs(long[] ns) {
    if (ns.length == 0) {
      return 0;
    }
    double s = 0;
    for (long v : ns) {
      s += v;
    }
    return s / ns.length / 1e6;
  }

  /** Odchylenie probkowe (dzielnik n-1), w ms. */
  private static double stddevMs(long[] ns) {
    if (ns.length < 2) {
      return 0;
    }
    final double m = meanMs(ns);
    double ss = 0;
    for (long v : ns) {
      final double d = v / 1e6 - m;
      ss += d * d;
    }
    return Math.sqrt(ss / (ns.length - 1));
  }

  /** Surowe probki punktu pomiarowego - material zrodlowy, zostaje na stale obok CSV.
   *  Format linii: "w &lt;ns&gt;" zapis, "r &lt;ns&gt;" odczyt, "hop &lt;rtt&gt; &lt;c2l&gt; &lt;l2c&gt;" probka sondy.
   *  scal.sh laczy te pliki ze wszystkich wezlow klienckich i liczy statystyki z JEDNEJ puli
   *  (percentyli nie wolno usredniac miedzy procesami). */
  private static void dumpLat(String path, Result r) throws IOException {
    try (PrintWriter out = new PrintWriter(new FileWriter(path, false))) {
      for (long v : r.writeLatNanos) {
        out.println("w " + v);
      }
      for (long v : r.readLatNanos) {
        out.println("r " + v);
      }
      for (int i = 0; i < r.hopRttNanos.length; i++) {
        out.println("hop " + r.hopRttNanos[i] + " " + r.hopC2lNanos[i] + " " + r.hopL2cNanos[i]);
      }
    }
  }

  // ---- CSV / arg parsing --------------------------------------------------

  // Nowe kolumny DOPISANE na koncu, zeby stare indeksy kolumn w awk sie nie przesunely.
  // write_* to opoznienie commitu (io().send wraca po zatwierdzeniu i apply na liderze);
  // hop_* to sonda PING do lidera (c2l = klient->lider, l2c = lider->klient, patrz splitHops).
  private static final String CSV_HEADER =
      "run_id,rep,transport,cluster_size,mode,total,writers,readers,payload_bytes,conn,read_from,"
          + "duration_s,"
          + "write_tput_req_s,write_MB_s,write_p50_ms,write_p99_ms,"
          + "read_tput_req_s,read_MB_s,read_p50_ms,read_p99_ms,"
          + "write_mean_ms,write_stddev_ms,read_mean_ms,read_stddev_ms,"
          + "requests_sent,requests_committed,requests_failed,conn_failed,reads_ok,reads_failed,"
          + "hop_c2l_ms,hop_l2c_ms,hop_rtt_ms";

  private static String csvRow(Mode mode, int payloadSize, ConnMode conn, ReadFrom readFrom, Result r) {
    final double durationS = r.durationNanos / 1e9;
    final int writes = r.writeLatNanos.length;   // = zadania ZATWIERDZONE (probka tylko z sukcesu)
    final int reads = r.readLatNanos.length;     // = odczyty udane
    final double writeTput = durationS > 0 ? writes / durationS : 0;
    final double readTput = durationS > 0 ? reads / durationS : 0;
    final double writeMB = durationS > 0
        ? (writes * (double) payloadSize) / (1024 * 1024) / durationS : 0;
    final double readMB = durationS > 0
        ? (reads * (double) payloadSize) / (1024 * 1024) / durationS : 0;
    // Liczniki kontrolne zlicza tylko rywrites; w scaling (tryb wycofany) zostaja neutralne.
    final long sent = mode == Mode.RYWRITES ? r.sent : writes;
    final long failed = sent - writes;
    final long readsFailed = mode == Mode.RYWRITES ? (long) writes - reads : 0;
    return String.format(Locale.ROOT,
        "%s,%d,%s,%d,%s,%d,%d,%d,%d,%s,%s,%.3f,%.1f,%.2f,%.3f,%.3f,%.1f,%.2f,%.3f,%.3f,"
            + "%.3f,%.3f,%.3f,%.3f,%d,%d,%d,%d,%d,%d,%.3f,%.3f,%.3f",
        runId, rep, transport, Constants.PEERS.size(),
        mode.name().toLowerCase(Locale.ROOT), r.total, r.writers, r.readers, payloadSize, conn,
        readFrom.name().toLowerCase(Locale.ROOT), durationS,
        writeTput, writeMB, percentileMs(r.writeLatNanos, 50), percentileMs(r.writeLatNanos, 99),
        readTput, readMB, percentileMs(r.readLatNanos, 50), percentileMs(r.readLatNanos, 99),
        meanMs(r.writeLatNanos), stddevMs(r.writeLatNanos),
        meanMs(r.readLatNanos), stddevMs(r.readLatNanos),
        sent, writes, failed, r.connFailed, reads, readsFailed,
        meanMs(r.hopC2lNanos), meanMs(r.hopL2cNanos), meanMs(r.hopRttNanos));
  }

  /** Parses sizes like "64", "1kB", "1MB" (case-insensitive) into bytes. */
  private static int parseSize(String s) {
    final String t = s.trim().toLowerCase(Locale.ROOT);
    if (t.endsWith("mb")) {
      return (int) (Double.parseDouble(t.substring(0, t.length() - 2)) * 1024 * 1024);
    } else if (t.endsWith("kb")) {
      return (int) (Double.parseDouble(t.substring(0, t.length() - 2)) * 1024);
    } else if (t.endsWith("b")) {
      return Integer.parseInt(t.substring(0, t.length() - 1));
    }
    return Integer.parseInt(t);
  }

  private static String opt(Map<String, String> m, String key, String def) {
    return m.getOrDefault(key, def);
  }

  public static void main(String[] args) {
    // --- parse --key value options ---
    final Map<String, String> o = new java.util.HashMap<>();
    for (int i = 0; i + 1 < args.length; i += 2) {
      if (args[i].startsWith("--")) {
        o.put(args[i].substring(2), args[i + 1]);
      }
    }
    final long tMain = System.nanoTime();
    try {
      transport = Transport.valueOf(opt(o, "transport", "TCP_TLS").toUpperCase(Locale.ROOT));
      runId = opt(o, "run-id", "-");
      rep = Integer.parseInt(opt(o, "rep", "1"));
      final ConnMode conn = ConnMode.valueOf(opt(o, "conn", "B").toUpperCase(Locale.ROOT));
      final ReadFrom readFrom = ReadFrom.valueOf(opt(o, "read-from", "followers").toUpperCase(Locale.ROOT));
      final Mode mode = Mode.valueOf(opt(o, "mode", "scaling").toUpperCase(Locale.ROOT));
      final double readRatio = Double.parseDouble(opt(o, "read-ratio", "0.7"));
      final int payloadSize = parseSize(opt(o, "payload", "64"));
      final int requests = Integer.parseInt(opt(o, "requests", "1000"));
      final int warmup = Integer.parseInt(opt(o, "warmup", "20"));
      final String csvPath = o.get("csv");
      final String latPath = o.get("lat-file");
      final int[] range = parseClients(opt(o, "clients", "5:30:5"));
      final int workerOffset = Integer.parseInt(opt(o, "worker-offset", "0"));
      if (workerOffset < 0) {
        throw new IllegalArgumentException("--worker-offset must be >= 0: " + workerOffset);
      }

      // rywrites has no writer/reader split, so read-ratio is irrelevant there.
      final String modeSpecific = (mode == Mode.RYWRITES)
          ? "read=own-writes-from-follower worker-offset=" + workerOffset
          : String.format(Locale.ROOT, "read-from=%s read-ratio=%.2f",
              readFrom.name().toLowerCase(Locale.ROOT), readRatio);
      System.out.printf("RaftBench: transport=%s mode=%s conn=%s %s "
              + "payload=%dB clients/workers=%s requests=%d warmup=%d%n",
          transport, mode.name().toLowerCase(Locale.ROOT), conn, modeSpecific,
          payloadSize, Arrays.toString(clientCounts(range)), requests, warmup);

      // --- discover leader + followers ---
      final RaftPeerId leaderId;
      try (RaftClient probe = newClient()) {
        probe.io().send(buildWrite(-1, 0));
        leaderId = probe.getLeaderId();
      }
      final List<RaftPeerId> followers = Constants.PEERS.stream()
          .map(RaftPeer::getId)
          .filter(id -> !id.equals(leaderId))
          .collect(Collectors.toList());
      System.out.println("Leader = " + leaderId + ", Followers = " + followers);

      // --- CSV writer (append; write header if new) ---
      PrintWriter csv = null;
      if (csvPath != null) {
        final boolean isNew = !new File(csvPath).exists();
        csv = new PrintWriter(new FileWriter(csvPath, true));
        if (isNew) {
          csv.println(CSV_HEADER);
        }
      }
      System.out.println(CSV_HEADER);

      // --- sweep the total client count, split into readers/writers ---
      final int[] counts = clientCounts(range);
      for (final int total : counts) {
        final Result r;
        final ReadFrom rowReadFrom;
        if (mode == Mode.RYWRITES) {
          r = runBenchRywrites(total, workerOffset, payloadSize, requests, warmup, conn,
              leaderId, followers);
          rowReadFrom = ReadFrom.FOLLOWERS;   // reads always come from a follower in rywrites
        } else {
          final int readers = Math.max(1, (int) Math.round(total * readRatio));
          final int writers = Math.max(1, total - readers);
          r = runBench(total, writers, readers, payloadSize, requests, warmup,
              conn, readFrom, leaderId, followers);
          rowReadFrom = readFrom;
        }
        final String row = csvRow(mode, payloadSize, conn, rowReadFrom, r);
        System.out.println(row);
        if (csv != null) {
          csv.println(row);
          csv.flush();
        }
        if (mode == Mode.RYWRITES) {
          // Kontrola "nic nie zginelo" w logu na zywo (te same liczby ida do CSV).
          final long committed = r.writeLatNanos.length;
          System.out.printf(Locale.ROOT,
              "counters: sent=%d committed=%d failed=%d conn_failed=%d reads_ok=%d "
                  + "reads_failed=%d hop_samples=%d%n",
              r.sent, committed, r.sent - committed, r.connFailed, r.readLatNanos.length,
              committed - r.readLatNanos.length, r.hopRttNanos.length);
        }
        if (latPath != null) {
          // Przy sweepie kilku liczb klientow kazdy punkt dostaje wlasny plik.
          dumpLat(counts.length > 1 ? latPath + "." + total : latPath, r);
        }
      }
      if (csv != null) {
        csv.close();
      }
      System.out.printf(Locale.ROOT, "Sweep elapsed: %.1f s%n", (System.nanoTime() - tMain) / 1e9);
      System.out.println("Done.");
      Runtime.getRuntime().halt(0);
    } catch (Throwable e) {
      e.printStackTrace();
      System.err.println();
      System.err.println("Usage: RaftBench --transport {TCP_TLS|QUIC} --mode {scaling|rywrites} "
          + "--clients FROM:TO:STEP --read-ratio R --read-from {leader|followers} "
          + "--payload SIZE --requests N --conn {A|B} [--warmup W] [--csv FILE] "
          + "[--run-id S] [--rep N] [--worker-offset K] [--lat-file F]");
      Runtime.getRuntime().halt(1);
    }
  }

  private static int[] parseClients(String s) {
    final String[] p = s.split(":");
    if (p.length == 1) {
      final int n = Integer.parseInt(p[0].trim());
      return new int[] {n, n, 1};
    }
    return new int[] {
        Integer.parseInt(p[0].trim()),
        Integer.parseInt(p[1].trim()),
        p.length > 2 ? Integer.parseInt(p[2].trim()) : 1};
  }

  private static int[] clientCounts(int[] range) {
    final List<Integer> list = new ArrayList<>();
    for (int n = range[0]; n <= range[1]; n += Math.max(1, range[2])) {
      list.add(n);
    }
    return list.stream().mapToInt(Integer::intValue).toArray();
  }

  private RaftBench() {
  }
}
