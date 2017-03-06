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
package org.apache.ratis.netty;

import org.apache.ratis.conf.ConfUtils;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static org.apache.ratis.conf.ConfUtils.requireMax;
import static org.apache.ratis.conf.ConfUtils.requireMin;

public interface NettyConfigKeys {
  String PREFIX = "raft.netty";

  interface Server {
    String PREFIX = NettyConfigKeys.PREFIX + ".server";

    String PORT_KEY = PREFIX + ".port";
    int PORT_DEFAULT = 0;

    static int port(BiFunction<String, Integer, Integer> getInt) {
      return ConfUtils.getInt(getInt,
          PORT_KEY, PORT_DEFAULT, requireMin(0), requireMax(65536));
    }

    static void setPort(BiConsumer<String, Integer> setInt, int port) {
      ConfUtils.setInt(setInt, PORT_KEY, port);
    }
  }

  static void main(String[] args) {
    ConfUtils.printAll(NettyConfigKeys.class);
  }
}
