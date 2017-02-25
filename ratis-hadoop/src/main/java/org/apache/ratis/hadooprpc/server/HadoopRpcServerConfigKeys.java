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
package org.apache.ratis.hadooprpc.server;

import org.apache.ratis.conf.ConfUtils;

import java.net.InetSocketAddress;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public interface HadoopRpcServerConfigKeys {
  String PREFIX = "raft.hadooprpc";

  /** IPC server configurations */
  abstract class Ipc {
    public static final String PREFIX = HadoopRpcServerConfigKeys.PREFIX + ".ipc";

    public static final String ADDRESS_KEY = PREFIX + ".address";
    public static final int DEFAULT_PORT = 10718;
    public static final String ADDRESS_DEFAULT = "0.0.0.0:" + DEFAULT_PORT;

    public static final String HANDLERS_KEY = PREFIX + ".handlers";
    public static final int HANDLERS_DEFAULT = 10;

    public static int handlers(BiFunction<String, Integer, Integer> getInt) {
      return ConfUtils.getInt(getInt,
          HANDLERS_KEY, HANDLERS_DEFAULT, 1, null);
    }

    public static InetSocketAddress address(BiFunction<String, String, String> getTrimmed) {
      return ConfUtils.getInetSocketAddress(getTrimmed,
          ADDRESS_KEY, ADDRESS_DEFAULT);
    }

    public static void setAddress(
        BiConsumer<String, String> setString,
        String address) {
      ConfUtils.set(setString, ADDRESS_KEY, address);
    }
  }
}
