/*
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

import java.util.concurrent.atomic.AtomicLong;

/**
 * A long ID for RPC calls.
 *
 * This class is threadsafe.
 */
public final class CallId {
  private static final AtomicLong CALL_ID_COUNTER = new AtomicLong();

  /** @return the default value. */
  public static long getDefault() {
    return 0;
  }

  /** @return the current value. */
  public static long get() {
    return CALL_ID_COUNTER.get() & Long.MAX_VALUE;
  }

  /** @return the current value and then increment. */
  public static long getAndIncrement() {
    return CALL_ID_COUNTER.getAndIncrement() & Long.MAX_VALUE;
  }

  private CallId() {}
}