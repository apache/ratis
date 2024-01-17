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

import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.TransactionContext;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * Managing {@link TransactionContext}.
 */
class TransactionManager {
  private final ConcurrentMap<TermIndex, Supplier<TransactionContext>> contexts = new ConcurrentHashMap<>();

  TransactionContext get(TermIndex termIndex) {
    return Optional.ofNullable(contexts.get(termIndex)).map(Supplier::get).orElse(null);
  }

  TransactionContext computeIfAbsent(TermIndex termIndex, Supplier<TransactionContext> constructor) {
    return contexts.computeIfAbsent(termIndex, i -> constructor).get();
  }

  void remove(TermIndex termIndex) {
    contexts.remove(termIndex);
  }
}