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
package org.apache.ratis;

import org.apache.ratis.conf.ConfUtils;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public interface RaftConfigKeys {
  String PREFIX = "raft";

  interface Rpc {
    String PREFIX = RaftConfigKeys.PREFIX + ".rpc";

    String TYPE_KEY = PREFIX + ".type";
    RpcType TYPE_DEFAULT = RpcType.GRPC;

    static RpcType type(BiFunction<String, RpcType, RpcType> getRpcType) {
      return ConfUtils.get(getRpcType, TYPE_KEY, TYPE_DEFAULT);
    }

    static void setType(BiConsumer<String, RpcType> setRpcType, RpcType type) {
      ConfUtils.set(setRpcType, TYPE_KEY, type);
    }
  }
}
