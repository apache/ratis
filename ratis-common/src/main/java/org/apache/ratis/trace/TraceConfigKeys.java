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
package org.apache.ratis.trace;

import org.apache.ratis.conf.RaftProperties;

import java.util.function.Consumer;

import static org.apache.ratis.conf.ConfUtils.getBoolean;
import static org.apache.ratis.conf.ConfUtils.setBoolean;

public interface TraceConfigKeys {
  String PREFIX = "raft.otel.tracing";

  String ENABLED_KEY = PREFIX + ".enabled";
  boolean ENABLED_DEFAULT = false;

  static boolean enabled(RaftProperties properties, Consumer<String> logger) {
    return getBoolean(properties::getBoolean, ENABLED_KEY, ENABLED_DEFAULT, logger);
  }

  static boolean enabled(RaftProperties properties) {
    return enabled(properties, null);
  }

  static void setEnabled(RaftProperties properties, boolean enabled) {
    setBoolean(properties::setBoolean, ENABLED_KEY, enabled);
  }
}

