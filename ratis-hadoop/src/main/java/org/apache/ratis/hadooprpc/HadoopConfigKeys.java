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
package org.apache.ratis.hadooprpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.ratis.conf.ConfUtils;
import org.apache.ratis.conf.Parameters;

import java.net.InetSocketAddress;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static org.apache.ratis.conf.ConfUtils.requireMin;

/** Hadoop Rpc specific configuration properties. */
public interface HadoopConfigKeys {
  String PREFIX = "raft.hadooprpc";

  String CONF_KEY = PREFIX + ".conf";

  static Configuration getConf(
      BiFunction<String, Class<Configuration>, Configuration> getConf) {
    return getConf.apply(CONF_KEY, Configuration.class);
  }

  static void setConf(Parameters parameters, Configuration conf) {
    parameters.put(CONF_KEY, conf, Configuration.class);
  }

  /** IPC server configurations */
  interface Ipc {
    String PREFIX = HadoopConfigKeys.PREFIX + ".ipc";

    String ADDRESS_KEY = PREFIX + ".address";
    int DEFAULT_PORT = 10718;
    String ADDRESS_DEFAULT = "0.0.0.0:" + DEFAULT_PORT;

    String HANDLERS_KEY = PREFIX + ".handlers";
    int HANDLERS_DEFAULT = 10;

    static int handlers(BiFunction<String, Integer, Integer> getInt) {
      return ConfUtils.getInt(getInt,
          HANDLERS_KEY, HANDLERS_DEFAULT, requireMin(1));
    }

    static InetSocketAddress address(BiFunction<String, String, String> getTrimmed) {
      return ConfUtils.getInetSocketAddress(getTrimmed,
          ADDRESS_KEY, ADDRESS_DEFAULT);
    }

    static void setAddress(BiConsumer<String, String> setString, String address) {
      ConfUtils.set(setString, ADDRESS_KEY, address);
    }
  }
}
