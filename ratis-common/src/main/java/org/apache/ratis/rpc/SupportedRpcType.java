/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.ratis.rpc;

import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.util.ReflectionUtils;

/** The RPC types supported. */
public enum SupportedRpcType implements RpcType {
  NETTY("org.apache.ratis.netty.NettyFactory"),
  GRPC("org.apache.ratis.grpc.GrpcFactory"),
  HADOOP("org.apache.ratis.hadooprpc.HadoopFactory");

  /** Same as {@link #valueOf(String)} except that this method is case insensitive. */
  public static SupportedRpcType valueOfIgnoreCase(String s) {
    return valueOf(s.toUpperCase());
  }

  private static final Class<?>[] ARG_CLASSES = {Parameters.class};

  private final String factoryClassName;

  SupportedRpcType(String factoryClassName) {
    this.factoryClassName = factoryClassName;
  }

  @Override
  public RpcFactory newFactory(RaftProperties properties, Parameters parameters) {
    final Class<? extends RpcFactory> clazz = ReflectionUtils.getClass(
        factoryClassName, properties, RpcFactory.class);
    return ReflectionUtils.newInstance(clazz, ARG_CLASSES, parameters);
  }
}
