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
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.Closeable;
import java.io.IOException;

public class Proxy<PROTOCOL> implements Closeable {
  public static <PROTOCOL> PROTOCOL getProxy(
      Class<PROTOCOL> clazz, String addressStr, Configuration conf)
      throws IOException {
    RPC.setProtocolEngine(conf, clazz, ProtobufRpcEngine.class);
    return RPC.getProxy(clazz, RPC.getProtocolVersion(clazz),
        org.apache.ratis.util.NetUtils.createSocketAddr(addressStr),
        UserGroupInformation.getCurrentUser(),
        conf, NetUtils.getSocketFactory(conf, clazz));
  }

  private final PROTOCOL protocol;

  public Proxy(Class<PROTOCOL> clazz, String addressStr, Configuration conf)
      throws IOException {
    this.protocol = getProxy(clazz, addressStr, conf);
  }

  public PROTOCOL getProtocol() {
    return protocol;
  }

  @Override
  public void close() {
    RPC.stopProxy(protocol);
  }
}
