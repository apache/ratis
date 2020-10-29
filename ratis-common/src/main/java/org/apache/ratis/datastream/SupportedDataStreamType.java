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
package org.apache.ratis.datastream;

import org.apache.ratis.conf.Parameters;
import org.apache.ratis.util.ReflectionUtils;

public enum SupportedDataStreamType implements DataStreamType {
  DISABLED("org.apache.ratis.client.DisabledDataStreamClientFactory",
      "org.apache.ratis.server.DisabledDataStreamServerFactory"),
  NETTY("org.apache.ratis.netty.NettyDataStreamFactory");

  private static final Class<?>[] ARG_CLASSES = {Parameters.class};

  public static SupportedDataStreamType valueOfIgnoreCase(String s) {
    return valueOf(s.toUpperCase());
  }

  private final String clientFactoryClassName;
  private final String serverFactoryClassName;

  SupportedDataStreamType(String clientFactoryClassName, String serverFactoryClassName) {
    this.clientFactoryClassName = clientFactoryClassName;
    this.serverFactoryClassName = serverFactoryClassName;
  }

  SupportedDataStreamType(String factoryClassName) {
    this(factoryClassName, factoryClassName);
  }

  @Override
  public DataStreamFactory newClientFactory(Parameters parameters) {
    final Class<? extends DataStreamFactory> clazz = ReflectionUtils.getClass(
        clientFactoryClassName, DataStreamFactory.class);
    return ReflectionUtils.newInstance(clazz, ARG_CLASSES, parameters);
  }

  @Override
  public DataStreamFactory newServerFactory(Parameters parameters) {
    final Class<? extends DataStreamFactory> clazz = ReflectionUtils.getClass(
        serverFactoryClassName, DataStreamFactory.class);
    return ReflectionUtils.newInstance(clazz, ARG_CLASSES, parameters);
  }
}
