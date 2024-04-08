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
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Managing {@link TransactionContext}.
 */
class TransactionManager {
  static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

  private final String name;
  private final ConcurrentMap<TermIndex, MemoizedSupplier<TransactionContext>> contexts = new ConcurrentHashMap<>();

  TransactionManager(Object name) {
    this.name = name + "-" + JavaUtils.getClassSimpleName(getClass());
  }

  @VisibleForTesting
  Map<TermIndex, MemoizedSupplier<TransactionContext>> getMap() {
    LOG.debug("{}", this);
    return Collections.unmodifiableMap(contexts);
  }

  TransactionContext get(TermIndex termIndex) {
    return Optional.ofNullable(contexts.get(termIndex)).map(MemoizedSupplier::get).orElse(null);
  }

  TransactionContext computeIfAbsent(TermIndex termIndex, MemoizedSupplier<TransactionContext> constructor) {
    final MemoizedSupplier<TransactionContext> m = contexts.computeIfAbsent(termIndex, i -> constructor);
    if (!m.isInitialized()) {
      LOG.debug("{}: {}", termIndex,  this);
    }
    return m.get();
  }

  void remove(TermIndex termIndex) {
    final MemoizedSupplier<TransactionContext> removed = contexts.remove(termIndex);
    if (removed != null) {
      LOG.debug("{}: {}", termIndex,  this);
    }
  }

  @Override
  public String toString() {
    if (contexts.isEmpty()) {
      return name + " <empty>";
    }

    final StringBuilder b = new StringBuilder(name);
    contexts.forEach((k, v) -> b.append("\n  ").append(k).append(": initialized? ").append(v.isInitialized()));
    return b.toString();
  }
}