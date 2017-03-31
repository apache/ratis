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

import static org.apache.ratis.conf.ConfUtils.get;
import static org.apache.ratis.conf.ConfUtils.printAll;
import static org.apache.ratis.conf.ConfUtils.set;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.util.ReflectionUtils;

public interface RaftConfigKeys {
  String PREFIX = "raft";

  interface Rpc {
    String PREFIX = RaftConfigKeys.PREFIX + ".rpc";

    String TYPE_KEY = PREFIX + ".type";
    String TYPE_DEFAULT = SupportedRpcType.GRPC.name();

    static RpcType type(RaftProperties properties) {
      final String t = get(properties::get, TYPE_KEY, TYPE_DEFAULT);

      try { // Try parsing it as a SupportedRpcType
        return SupportedRpcType.valueOfIgnoreCase(t);
      } catch(IllegalArgumentException iae) {
      }

      // Try using it as a class name
      return ReflectionUtils.newInstance(
          ReflectionUtils.getClass(t, properties, RpcType.class));
    }

    static void setType(RaftProperties properties, RpcType type) {
      set(properties::set, TYPE_KEY, type.name());
    }
  }

  static void main(String[] args) {
    printAll(RaftConfigKeys.class);
  }
}
