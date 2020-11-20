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
package org.apache.ratis.server.impl;

import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.server.DataStreamMap;
import org.apache.ratis.statemachine.StateMachine.DataStream;
import org.apache.ratis.util.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

class DataStreamMapImpl implements DataStreamMap {
  public static final Logger LOG = LoggerFactory.getLogger(DataStreamMapImpl.class);

  private final String name;
  private final ConcurrentMap<ClientInvocationId, CompletableFuture<DataStream>> map = new ConcurrentHashMap<>();

  DataStreamMapImpl(Object name) {
    this.name = name + "-" + JavaUtils.getClassSimpleName(getClass());
  }

  @Override
  public CompletableFuture<DataStream> remove(ClientInvocationId invocationId) {
    return map.remove(invocationId);
  }

  @Override
  public CompletableFuture<DataStream> computeIfAbsent(ClientInvocationId invocationId,
      Function<ClientInvocationId, CompletableFuture<DataStream>> newDataStream) {
    return map.computeIfAbsent(invocationId, newDataStream);
  }

  @Override
  public String toString() {
    return name;
  }
}
