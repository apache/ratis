/**
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
package org.apache.ratis.server;

import org.apache.ratis.RpcType;
import org.apache.ratis.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public interface RaftServerConfigKeys {

  String PREFIX = "raft.server";

  /** IPC server configurations */
  interface Ipc {
    String PREFIX  = RaftServerConfigKeys.PREFIX + ".ipc";

    String ADDRESS_KEY = PREFIX + ".address";
    int    DEFAULT_PORT = 10718;
    String ADDRESS_DEFAULT = "0.0.0.0:" + DEFAULT_PORT;

    String HANDLERS_KEY = PREFIX + ".handlers";
    int    HANDLERS_DEFAULT = 10;

    class Getters {
      private final Get get;

      Getters(Get get) {
        this.get = get;
      }

      public int handlers() {
        return get.getInt(HANDLERS_KEY, HANDLERS_DEFAULT, 1, null);
      }

      public InetSocketAddress address() {
        return get.getInetSocketAddress(ADDRESS_KEY, ADDRESS_DEFAULT);
      }
    }
  }

  enum Factory {
    NETTY("org.apache.ratis.server.impl.ServerFactory$BaseFactory"),
    GRPC("org.apache.ratis.grpc.server.GrpcServerFactory"),
    HADOOP("org.apache.ratis.server.impl.ServerFactory$BaseFactory"),
    SIMULATED("org.apache.ratis.server.impl.ServerFactory$BaseFactory");

    public static String getKey(String rpcType) {
      return RaftServerConfigKeys.PREFIX + ".factory." + rpcType + ".class";
    }

    public static Factory valueOf(RpcType rpcType) {
      return valueOf(rpcType.name());
    }

    private final RpcType rpcType = RpcType.valueOf(name());
    private final String key = getKey(name().toLowerCase());
    private final String defaultClass;

    Factory(String defaultClass) {
      this.defaultClass = defaultClass;
    }

    public RpcType getRpcType() {
      return rpcType;
    }

    public String getKey() {
      return key;
    }

    public String getDefaultClass() {
      return defaultClass;
    }

    @Override
    public String toString() {
      return getRpcType() + ":" + getKey() + ":" + getDefaultClass();
    }
  }

  String RAFT_SERVER_USE_MEMORY_LOG_KEY = "raft.server.use.memory.log";
  boolean RAFT_SERVER_USE_MEMORY_LOG_DEFAULT = false;

  String RAFT_SERVER_STORAGE_DIR_KEY = "raft.server.storage.dir";
  String RAFT_SERVER_STORAGE_DIR_DEFAULT = "file:///tmp/raft-server/";

  /** whether trigger snapshot when log size exceeds limit */
  String RAFT_SERVER_AUTO_SNAPSHOT_ENABLED_KEY = "raft.server.auto.snapshot.enabled";
  /** by default let the state machine to decide when to do checkpoint */
  boolean RAFT_SERVER_AUTO_SNAPSHOT_ENABLED_DEFAULT = false;

  /** log size limit (in number of log entries) that triggers the snapshot */
  String RAFT_SERVER_SNAPSHOT_TRIGGER_THRESHOLD_KEY = "raft.server.snapshot.trigger.threshold";
  long RAFT_SERVER_SNAPSHOT_TRIGGER_THRESHOLD_DEFAULT = 400000;

  String RAFT_LOG_SEGMENT_MAX_SIZE_KEY = "raft.log.segment.max.size";
  long RAFT_LOG_SEGMENT_MAX_SIZE_DEFAULT = 1024L * 1024 * 8; // 8MB

  String RAFT_LOG_SEGMENT_PREALLOCATED_SIZE_KEY = "raft.log.segment.preallocated.size";
  int RAFT_LOG_SEGMENT_PREALLOCATED_SIZE_DEFAULT = 1024 * 1024 * 4; // 4MB

  String RAFT_LOG_WRITE_BUFFER_SIZE_KEY = "raft.log.write.buffer.size";
  int RAFT_LOG_WRITE_BUFFER_SIZE_DEFAULT = 64 * 1024;

  String RAFT_SNAPSHOT_CHUNK_MAX_SIZE_KEY = "raft.snapshot.chunk.max.size";
  int RAFT_SNAPSHOT_CHUNK_MAX_SIZE_DEFAULT = 1024 * 1024 * 16;

  String RAFT_LOG_FORCE_SYNC_NUM_KEY = "raft.log.force.sync.num";
  int RAFT_LOG_FORCE_SYNC_NUM_DEFAULT = 128;

  /** server rpc timeout related */
  String RAFT_SERVER_RPC_TIMEOUT_MIN_MS_KEY = "raft.server.rpc.timeout.min.ms";
  int RAFT_SERVER_RPC_TIMEOUT_MIN_MS_DEFAULT = 150;

  String RAFT_SERVER_RPC_TIMEOUT_MAX_MS_KEY = "raft.server.rpc.timeout.max.ms";
  int RAFT_SERVER_RPC_TIMEOUT_MAX_MS_DEFAULT = 300;

  String RAFT_SERVER_RPC_SLEEP_TIME_MS_KEY = "raft.server.rpc.sleep.time.ms";
  int RAFT_SERVER_RPC_SLEEP_TIME_MS_DEFAULT = 25;

  /**
   * When bootstrapping a new peer, If the gap between the match index of the
   * peer and the leader's latest committed index is less than this gap, we
   * treat the peer as caught-up.
   */
  String RAFT_SERVER_STAGING_CATCHUP_GAP_KEY = "raft.server.staging.catchup.gap";
  int RAFT_SERVER_STAGING_CATCHUP_GAP_DEFAULT = 1000; // increase this number when write throughput is high

  String RAFT_SERVER_LOG_APPENDER_BUFFER_CAPACITY_KEY = "raft.server.log.appender.buffer.capacity";
  int RAFT_SERVER_LOG_APPENDER_BUFFER_CAPACITY_DEFAULT = 4 * 1024 * 1024; // 4MB

  String RAFT_SERVER_LOG_APPENDER_BATCH_ENABLED_KEY = "raft.server.log.appender.batch.enabled";
  boolean RAFT_SERVER_LOG_APPENDER_BATCH_ENABLED_DEFAULT = false;

  /** An utility class to get conf values. */
  abstract class Get {
    static Logger LOG = LoggerFactory.getLogger(RaftServerConfigKeys.class);

    private final Ipc.Getters ipc = new Ipc.Getters(this);

    protected abstract int getInt(String key, int defaultValue);

    int getInt(String key, int defaultValue, Integer min, Integer max) {
      final int value = getInt(key, defaultValue);
      final String s = key + " = " + value;
      if (min != null && value < min) {
        throw new IllegalArgumentException(s + " < min = " + min);
      }
      if (max != null && value > max) {
        throw new IllegalArgumentException(s + " > max = " + max);
      }
      LOG.info(s);
      return value;
    }

    protected abstract String getTrimmed(String key, String defaultValue);

    InetSocketAddress getInetSocketAddress(String key, String defaultValue) {
      final String address = getTrimmed(key, defaultValue);
      LOG.info(key + " = " + address);
      return NetUtils.createSocketAddr(address);
    }

    public Ipc.Getters ipc() {
      return ipc;
    }
  }
}
