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
package org.apache.hadoop.raft.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
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

  String RAFT_SERVER_USE_MEMORY_LOG_KEY = "raft.server.use.memory.log";
  boolean RAFT_SERVER_USE_MEMORY_LOG_DEFAULT = false;

  String RAFT_SERVER_STORAGE_DIR_KEY = "raft.server.storage.dir";
  String RAFT_SERVER_STORAGE_DIR_DEFAULT = "file:///tmp/raft-server/";

  /** An utility class to get conf values. */
  class Get {
    static Logger LOG = LoggerFactory.getLogger(RaftServerConfigKeys.class);

    private final Configuration conf;
    private final Ipc.Getters ipc = new Ipc.Getters(this);

    public Get(Configuration conf) {
      this.conf = conf;
    }

    int getInt(String key, int defaultValue, Integer min, Integer max) {
      final int value = conf.getInt(key, defaultValue);
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

    InetSocketAddress getInetSocketAddress(String key, String defaultValue) {
      final String address = conf.getTrimmed(key, defaultValue);
      LOG.info(key + " = " + address);
      return NetUtils.createSocketAddr(address);
    }

    public Ipc.Getters ipc() {
      return ipc;
    }
  }
}
