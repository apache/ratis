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
package org.apache.ratis.quic;

import org.apache.ratis.conf.RaftProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static org.apache.ratis.conf.ConfUtils.*;

public interface QuicConfigKeys {

  String PREFIX = "raft.quic";

  /** QUIC application-level protocol identifier used in the TLS 1.3 ALPN extension. */
  String ALPN = "ratis-quic";

  // -------------------------------------------------------------------------
  // Server — P2P (server-to-server Raft consensus traffic)
  // -------------------------------------------------------------------------
  interface Server {
    Logger LOG = LoggerFactory.getLogger(Server.class);
    static Consumer<String> getDefaultLog() { return LOG::info; }

    String PREFIX = QuicConfigKeys.PREFIX + ".server";

    String HOST_KEY = PREFIX + ".host";
    String HOST_DEFAULT = null;

    String PORT_KEY = PREFIX + ".port";
    int PORT_DEFAULT = 0;

    /**
     * Path to the PEM-encoded X.509 certificate presented to peers.
     * When null, a temporary SelfSignedCertificate is generated at startup.
     */
    String TLS_CERT_KEY = PREFIX + ".tls.cert";
    String TLS_CERT_DEFAULT = null;

    /**
     * Path to the PEM-encoded PKCS#8 private key matching the certificate.
     * When null, a temporary SelfSignedCertificate is generated at startup.
     */
    String TLS_KEY_KEY = PREFIX + ".tls.key";
    String TLS_KEY_DEFAULT = null;

    static String host(RaftProperties properties) {
      return get(properties::get, HOST_KEY, HOST_DEFAULT, getDefaultLog());
    }
    static void setHost(RaftProperties properties, String host) {
      set(properties::set, HOST_KEY, host);
    }

    static int port(RaftProperties properties) {
      return getInt(properties::getInt, PORT_KEY, PORT_DEFAULT,
          getDefaultLog(), requireMin(0), requireMax(65536));
    }
    static void setPort(RaftProperties properties, int port) {
      setInt(properties::setInt, PORT_KEY, port);
    }

    static String tlsCert(RaftProperties properties) {
      return get(properties::get, TLS_CERT_KEY, TLS_CERT_DEFAULT, getDefaultLog());
    }
    static void setTlsCert(RaftProperties properties, String path) {
      set(properties::set, TLS_CERT_KEY, path);
    }

    static String tlsKey(RaftProperties properties) {
      return get(properties::get, TLS_KEY_KEY, TLS_KEY_DEFAULT, getDefaultLog());
    }
    static void setTlsKey(RaftProperties properties, String path) {
      set(properties::set, TLS_KEY_KEY, path);
    }

    /**
     * Stream layout of the server-to-server connections this server opens to its peers.
     * {@code false} (default): one persistent stream per message type (AppendEntries,
     * heartbeat, InstallSnapshot, RequestVote, other), so a heartbeat never waits behind
     * a large log batch. {@code true}: a single persistent stream carries every message
     * type in one ordered sequence, the way a TCP connection does, and the head-of-line
     * blocking between message types returns. The single-stream layout is meant for
     * benchmarks that isolate the contribution of the per-type layout; external Raft
     * clients are unaffected, as they always use one stream per connection.
     */
    String SINGLE_STREAM_KEY = PREFIX + ".single-stream";
    boolean SINGLE_STREAM_DEFAULT = false;

    static boolean singleStream(RaftProperties properties) {
      return getBoolean(properties::getBoolean, SINGLE_STREAM_KEY,
          SINGLE_STREAM_DEFAULT, getDefaultLog());
    }
    static void setSingleStream(RaftProperties properties, boolean singleStream) {
      setBoolean(properties::setBoolean, SINGLE_STREAM_KEY, singleStream);
    }
  }

  // -------------------------------------------------------------------------
  // Client — outgoing P2P connections (this server connecting to peers)
  // -------------------------------------------------------------------------
  interface Client {
    Logger LOG = LoggerFactory.getLogger(Client.class);
    static Consumer<String> getDefaultLog() { return LOG::debug; }

    String PREFIX = QuicConfigKeys.PREFIX + ".client";

    /**
     * Path to the PEM-encoded CA certificate used to verify remote peers.
     * When null and tlsInsecure is false, TLS handshake will fail unless
     * the server uses a publicly-trusted certificate.
     */
    String TLS_CA_CERT_KEY = PREFIX + ".tls.ca-cert";
    String TLS_CA_CERT_DEFAULT = null;

    /** Path to PEM-encoded client certificate for mutual TLS. Optional. */
    String TLS_CERT_KEY = PREFIX + ".tls.cert";
    String TLS_CERT_DEFAULT = null;

    /** Path to PEM-encoded private key for mutual TLS. Optional. */
    String TLS_KEY_KEY = PREFIX + ".tls.key";
    String TLS_KEY_DEFAULT = null;

    /**
     * Skip server certificate verification.
     * MUST NOT be true in production — use only in local/test environments
     * where SelfSignedCertificate is in use.
     */
    String TLS_INSECURE_KEY = PREFIX + ".tls.insecure";
    boolean TLS_INSECURE_DEFAULT = false;

    static String tlsCaCert(RaftProperties properties) {
      return get(properties::get, TLS_CA_CERT_KEY, TLS_CA_CERT_DEFAULT, getDefaultLog());
    }
    static void setTlsCaCert(RaftProperties properties, String path) {
      set(properties::set, TLS_CA_CERT_KEY, path);
    }

    static String tlsCert(RaftProperties properties) {
      return get(properties::get, TLS_CERT_KEY, TLS_CERT_DEFAULT, getDefaultLog());
    }
    static void setTlsCert(RaftProperties properties, String path) {
      set(properties::set, TLS_CERT_KEY, path);
    }

    static String tlsKey(RaftProperties properties) {
      return get(properties::get, TLS_KEY_KEY, TLS_KEY_DEFAULT, getDefaultLog());
    }
    static void setTlsKey(RaftProperties properties, String path) {
      set(properties::set, TLS_KEY_KEY, path);
    }

    static boolean tlsInsecure(RaftProperties properties) {
      return getBoolean(properties::getBoolean, TLS_INSECURE_KEY,
          TLS_INSECURE_DEFAULT, getDefaultLog());
    }
    static void setTlsInsecure(RaftProperties properties, boolean insecure) {
      setBoolean(properties::setBoolean, TLS_INSECURE_KEY, insecure);
    }
  }

  // -------------------------------------------------------------------------
  // ClientServer — separate UDP port accepting external Raft-client requests.
  // Each incoming RaftClientRequest gets its own short-lived QUIC stream (HTTP/3
  // model) to eliminate head-of-line blocking between unrelated client calls.
  // -------------------------------------------------------------------------
  interface ClientServer {
    Logger LOG = LoggerFactory.getLogger(ClientServer.class);
    static Consumer<String> getDefaultLog() { return LOG::info; }

    String PREFIX = QuicConfigKeys.PREFIX + ".client-server";

    String HOST_KEY = PREFIX + ".host";
    String HOST_DEFAULT = null;

    String PORT_KEY = PREFIX + ".port";
    int PORT_DEFAULT = 0;

    static String host(RaftProperties properties) {
      return get(properties::get, HOST_KEY, HOST_DEFAULT, getDefaultLog());
    }
    static void setHost(RaftProperties properties, String host) {
      set(properties::set, HOST_KEY, host);
    }

    static int port(RaftProperties properties) {
      return getInt(properties::getInt, PORT_KEY, PORT_DEFAULT,
          getDefaultLog(), requireMin(0), requireMax(65536));
    }
    static void setPort(RaftProperties properties, int port) {
      setInt(properties::setInt, PORT_KEY, port);
    }
  }

  static void main(String[] args) {
    printAll(QuicConfigKeys.class);
  }
}
