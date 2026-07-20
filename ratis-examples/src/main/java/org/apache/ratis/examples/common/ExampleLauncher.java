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
package org.apache.ratis.examples.common;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.trace.TraceConfigKeys;
import org.apache.ratis.util.StringUtils;

/**
 * Applies example-only JVM system properties to {@link RaftProperties}.
 * {@link Runner} calls {@link #init()} at startup; example subcommands obtain
 * configured properties via {@link #newRaftProperties()}.
 */
public final class ExampleLauncher {

  private static volatile boolean initialized;
  private static Boolean tracingEnabled;

  private ExampleLauncher() {
  }

  /** Reads example JVM system properties. Called once from {@link Runner#main}. */
  public static void init() {
    initFromSystemProperties();
  }

  /**
   * Creates {@link RaftProperties} for examples, applying any tracing configuration
   * read from the JVM system property {@value TraceConfigKeys#ENABLED_KEY}.
   */
  public static RaftProperties newRaftProperties() {
    initFromSystemProperties();
    final RaftProperties properties = new RaftProperties();
    if (tracingEnabled != null) {
      TraceConfigKeys.setEnabled(properties, tracingEnabled);
    }
    return properties;
  }

  private static void initFromSystemProperties() {
    if (initialized) {
      return;
    }
    synchronized (ExampleLauncher.class) {
      if (initialized) {
        return;
      }
      final String value = System.getProperty(TraceConfigKeys.ENABLED_KEY);
      if (value != null) {
        tracingEnabled = StringUtils.string2boolean(value.trim(), TraceConfigKeys.ENABLED_DEFAULT);
      }
      initialized = true;
    }
  }
}
