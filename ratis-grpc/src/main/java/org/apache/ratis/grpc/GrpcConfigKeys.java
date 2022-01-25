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
package org.apache.ratis.grpc;

import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.util.SizeInBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static org.apache.ratis.conf.ConfUtils.*;

public interface GrpcConfigKeys {
  Logger LOG = LoggerFactory.getLogger(GrpcConfigKeys.class);
  static Consumer<String> getDefaultLog() {
    return LOG::info;
  }

  String PREFIX = "raft.grpc";

  interface TLS {
    String PREFIX = GrpcConfigKeys.PREFIX + ".tls";

    String ENABLED_KEY = PREFIX + ".enabled";
    boolean ENABLED_DEFAULT = false;
    static boolean enabled(RaftProperties properties) {
      return getBoolean(properties::getBoolean, ENABLED_KEY, ENABLED_DEFAULT, getDefaultLog());
    }
    static void setEnabled(RaftProperties properties, boolean enabled) {
      setBoolean(properties::setBoolean, ENABLED_KEY, enabled);
    }

    String MUTUAL_AUTHN_ENABLED_KEY = PREFIX + ".mutual_authn.enabled";
    boolean MUTUAL_AUTHN_ENABLED_DEFAULT = false;
    static boolean mutualAuthnEnabled(RaftProperties properties) {
      return getBoolean(properties::getBoolean,
          MUTUAL_AUTHN_ENABLED_KEY, MUTUAL_AUTHN_ENABLED_DEFAULT, getDefaultLog());
    }
    static void setMutualAuthnEnabled(RaftProperties properties, boolean mutualAuthnEnabled) {
      setBoolean(properties::setBoolean, MUTUAL_AUTHN_ENABLED_KEY, mutualAuthnEnabled);
    }

    String PRIVATE_KEY_FILE_NAME_KEY = PREFIX + ".private.key.file.name";
    String PRIVATE_KEY_FILE_NAME_DEFAULT = "private.pem";
    static String privateKeyFileName(RaftProperties properties) {
      return get(properties::get, PRIVATE_KEY_FILE_NAME_KEY, PRIVATE_KEY_FILE_NAME_DEFAULT, getDefaultLog());
    }
    static void setPrivateKeyFileName(RaftProperties properties, String privateKeyFileName) {
      set(properties::set, PRIVATE_KEY_FILE_NAME_KEY, privateKeyFileName);
    }

    String CERT_CHAIN_FILE_NAME_KEY = PREFIX + ".cert.chain.file.name";
    String CERT_CHAIN_FILE_NAME_DEFAULT = "certificate.crt";
    static String certChainFileName(RaftProperties properties) {
      return get(properties::get, CERT_CHAIN_FILE_NAME_KEY, CERT_CHAIN_FILE_NAME_DEFAULT, getDefaultLog());
    }
    static void setCertChainFileName(RaftProperties properties, String certChainFileName) {
      set(properties::set, CERT_CHAIN_FILE_NAME_KEY, certChainFileName);
    }

    String TRUST_STORE_KEY = PREFIX + ".trust.store";
    String TRUST_STORE_DEFAULT = "ca.crt";
    static String trustStore(RaftProperties properties) {
      return get(properties::get, TRUST_STORE_KEY, TRUST_STORE_DEFAULT, getDefaultLog());
    }
    static void setTrustStore(RaftProperties properties, String trustStore) {
      set(properties::set, TRUST_STORE_KEY, trustStore);
    }

    String CONF_PARAMETER = PREFIX + ".conf";
    Class<GrpcTlsConfig> CONF_CLASS = GrpcTlsConfig.class;
    static GrpcTlsConfig conf(Parameters parameters) {
      return parameters != null ? parameters.get(CONF_PARAMETER, CONF_CLASS): null;
    }
    static void setConf(Parameters parameters, GrpcTlsConfig conf) {
      parameters.put(CONF_PARAMETER, conf, GrpcTlsConfig.class);
    }
  }

  interface Admin {
    String PREFIX = GrpcConfigKeys.PREFIX + ".admin";

    String PORT_KEY = PREFIX + ".port";
    int PORT_DEFAULT = -1;
    static int port(RaftProperties properties) {
      final int port = getInt(properties::getInt,
          PORT_KEY, PORT_DEFAULT, getDefaultLog(), requireMin(-1), requireMax(65536));
      return port != PORT_DEFAULT ? port : Server.port(properties);
    }
    static void setPort(RaftProperties properties, int port) {
      setInt(properties::setInt, PORT_KEY, port);
    }

    String TLS_CONF_PARAMETER = PREFIX + ".tls.conf";
    Class<GrpcTlsConfig> TLS_CONF_CLASS = TLS.CONF_CLASS;
    static GrpcTlsConfig tlsConf(Parameters parameters) {
      return parameters != null ? parameters.get(TLS_CONF_PARAMETER, TLS_CONF_CLASS): null;
    }
    static void setTlsConf(Parameters parameters, GrpcTlsConfig conf) {
      parameters.put(TLS_CONF_PARAMETER, conf, TLS_CONF_CLASS);
    }
  }

  interface Client {
    String PREFIX = GrpcConfigKeys.PREFIX + ".client";

    String PORT_KEY = PREFIX + ".port";
    int PORT_DEFAULT = -1;
    static int port(RaftProperties properties) {
      final int port = getInt(properties::getInt,
          PORT_KEY, PORT_DEFAULT, getDefaultLog(), requireMin(-1), requireMax(65536));
      return port != PORT_DEFAULT ? port : Server.port(properties);
    }
    static void setPort(RaftProperties properties, int port) {
      setInt(properties::setInt, PORT_KEY, port);
    }

    String TLS_CONF_PARAMETER = PREFIX + ".tls.conf";
    Class<GrpcTlsConfig> TLS_CONF_CLASS = TLS.CONF_CLASS;
    static GrpcTlsConfig tlsConf(Parameters parameters) {
      return parameters != null ? parameters.get(TLS_CONF_PARAMETER, TLS_CONF_CLASS): null;
    }
    static void setTlsConf(Parameters parameters, GrpcTlsConfig conf) {
      parameters.put(TLS_CONF_PARAMETER, conf, TLS_CONF_CLASS);
    }
  }

  interface Server {
    String PREFIX = GrpcConfigKeys.PREFIX + ".server";

    String PORT_KEY = PREFIX + ".port";
    int PORT_DEFAULT = 0;
    static int port(RaftProperties properties) {
      return getInt(properties::getInt,
          PORT_KEY, PORT_DEFAULT, getDefaultLog(), requireMin(0), requireMax(65536));
    }
    static void setPort(RaftProperties properties, int port) {
      setInt(properties::setInt, PORT_KEY, port);
    }

    String ASYNC_REQUEST_THREAD_POOL_CACHED_KEY = PREFIX + ".async.request.thread.pool.cached";
    boolean ASYNC_REQUEST_THREAD_POOL_CACHED_DEFAULT = true;
    static boolean asyncRequestThreadPoolCached(RaftProperties properties) {
      return getBoolean(properties::getBoolean, ASYNC_REQUEST_THREAD_POOL_CACHED_KEY,
          ASYNC_REQUEST_THREAD_POOL_CACHED_DEFAULT, getDefaultLog());
    }
    static void setAsyncRequestThreadPoolCached(RaftProperties properties, boolean useCached) {
      setBoolean(properties::setBoolean, ASYNC_REQUEST_THREAD_POOL_CACHED_KEY, useCached);
    }

    String ASYNC_REQUEST_THREAD_POOL_SIZE_KEY = PREFIX + ".async.request.thread.pool.size";
    int ASYNC_REQUEST_THREAD_POOL_SIZE_DEFAULT = 32;
    static int asyncRequestThreadPoolSize(RaftProperties properties) {
      return getInt(properties::getInt, ASYNC_REQUEST_THREAD_POOL_SIZE_KEY,
          ASYNC_REQUEST_THREAD_POOL_SIZE_DEFAULT, getDefaultLog(),
          requireMin(0), requireMax(65536));
    }
    static void setAsyncRequestThreadPoolSize(RaftProperties properties, int port) {
      setInt(properties::setInt, ASYNC_REQUEST_THREAD_POOL_SIZE_KEY, port);
    }

    String TLS_CONF_PARAMETER = PREFIX + ".tls.conf";
    Class<GrpcTlsConfig> TLS_CONF_CLASS = TLS.CONF_CLASS;
    static GrpcTlsConfig tlsConf(Parameters parameters) {
      return parameters != null ? parameters.get(TLS_CONF_PARAMETER, TLS_CONF_CLASS): null;
    }
    static void setTlsConf(Parameters parameters, GrpcTlsConfig conf) {
      parameters.put(TLS_CONF_PARAMETER, conf, TLS_CONF_CLASS);
    }

    String LEADER_OUTSTANDING_APPENDS_MAX_KEY = PREFIX + ".leader.outstanding.appends.max";
    int LEADER_OUTSTANDING_APPENDS_MAX_DEFAULT = 128;
    static int leaderOutstandingAppendsMax(RaftProperties properties) {
      return getInt(properties::getInt,
          LEADER_OUTSTANDING_APPENDS_MAX_KEY, LEADER_OUTSTANDING_APPENDS_MAX_DEFAULT, getDefaultLog(), requireMin(0));
    }
    static void setLeaderOutstandingAppendsMax(RaftProperties properties, int maxAppend) {
      setInt(properties::setInt, LEADER_OUTSTANDING_APPENDS_MAX_KEY, maxAppend);
    }
  }

  String MESSAGE_SIZE_MAX_KEY = PREFIX + ".message.size.max";
  SizeInBytes MESSAGE_SIZE_MAX_DEFAULT = SizeInBytes.valueOf("64MB");
  static SizeInBytes messageSizeMax(RaftProperties properties, Consumer<String> logger) {
    return getSizeInBytes(properties::getSizeInBytes,
        MESSAGE_SIZE_MAX_KEY, MESSAGE_SIZE_MAX_DEFAULT, logger);
  }
  static void setMessageSizeMax(RaftProperties properties, SizeInBytes maxMessageSize) {
    setSizeInBytes(properties::set, MESSAGE_SIZE_MAX_KEY, maxMessageSize);
  }

  String FLOW_CONTROL_WINDOW_KEY = PREFIX + ".flow.control.window";
  SizeInBytes FLOW_CONTROL_WINDOW_DEFAULT = SizeInBytes.valueOf("1MB");
  static SizeInBytes flowControlWindow(RaftProperties properties, Consumer<String> logger) {
    return getSizeInBytes(properties::getSizeInBytes,
        FLOW_CONTROL_WINDOW_KEY, FLOW_CONTROL_WINDOW_DEFAULT, logger);
  }
  static void setFlowControlWindow(RaftProperties properties, SizeInBytes flowControlWindowSize) {
    setSizeInBytes(properties::set, FLOW_CONTROL_WINDOW_KEY, flowControlWindowSize);
  }

  static void main(String[] args) {
    printAll(GrpcConfigKeys.class);
  }
}
