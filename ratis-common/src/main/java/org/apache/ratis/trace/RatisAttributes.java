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
package org.apache.ratis.trace;

import io.opentelemetry.api.common.AttributeKey;

/**
 * The constants in this class correspond with the guidance outlined by the OpenTelemetry <a href=
 * "https://github.com/open-telemetry/semantic-conventions">Semantic
 * Conventions</a>.
 */
public final class RatisAttributes {
  public static final AttributeKey<String> CLIENT_ID = AttributeKey.stringKey("raft.client.id");
  public static final AttributeKey<String> MEMBER_ID = AttributeKey.stringKey("raft.member.id");
  public static final AttributeKey<String> CALL_ID = AttributeKey.stringKey("raft.call.id");

  public static final AttributeKey<String> PEER_ID = AttributeKey.stringKey("raft.peer.id");
  public static final AttributeKey<String> OPERATION_NAME = AttributeKey.stringKey("raft.operation.name");
  public static final AttributeKey<String> OPERATION_TYPE = AttributeKey.stringKey("raft.operation.type");

  /** Number of log entries in a single {@code AppendEntries} RPC (0 for heartbeat). */
  public static final AttributeKey<Long> APPEND_ENTRIES_COUNT = AttributeKey.longKey("raft.append.entries.count");

  private RatisAttributes() {
  }
}
