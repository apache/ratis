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
package org.apache.ratis.shell.cli.sh.command;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.retry.ExponentialBackoffRetry;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.thirdparty.com.google.common.io.Closer;
import org.apache.ratis.util.TimeDuration;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * A context for ratis-shell.
 */
public final class Context implements Closeable {
  private static final TimeDuration DEFAULT_REQUEST_TIMEOUT = TimeDuration.valueOf(15, TimeUnit.SECONDS);
  private static final RetryPolicy DEFAULT_RETRY_POLICY = ExponentialBackoffRetry.newBuilder()
      .setBaseSleepTime(TimeDuration.valueOf(1000, TimeUnit.MILLISECONDS))
      .setMaxAttempts(10)
      .setMaxSleepTime(TimeDuration.valueOf(100_000, TimeUnit.MILLISECONDS))
      .build();

  private final PrintStream mPrintStream;
  private final Closer mCloser;

  private final boolean cli;
  private final RetryPolicy retryPolicy;
  private final RaftProperties properties;
  private final Parameters parameters;

  /**
   * Build a context.
   * @param printStream the print stream
   */
  public Context(PrintStream printStream) {
    this(printStream, true, DEFAULT_RETRY_POLICY, new RaftProperties(), null);
  }

  public Context(PrintStream printStream, boolean cli, RetryPolicy retryPolicy,
      RaftProperties properties, Parameters parameters) {
    mCloser = Closer.create();
    mPrintStream = mCloser.register(Objects.requireNonNull(printStream, "printStream == null"));

    this.cli = cli;
    this.retryPolicy = retryPolicy != null? retryPolicy : DEFAULT_RETRY_POLICY;
    this.properties = properties != null? properties : new RaftProperties();
    this.parameters = parameters;
  }

  /**
   * @return the print stream to write to
   */
  public PrintStream getPrintStream() {
    return mPrintStream;
  }

  /** Is this from CLI? */
  public boolean isCli() {
    return cli;
  }

  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  public RaftProperties getProperties() {
    return properties;
  }

  public Parameters getParameters() {
    return parameters;
  }

  /** Create a new {@link RaftClient} from the given group. */
  public RaftClient newRaftClient(RaftGroup group) {
    final RaftProperties properties = getProperties();
    if (isCli()) {
      RaftClientConfigKeys.Rpc.setRequestTimeout(properties, DEFAULT_REQUEST_TIMEOUT);

      // Since ratis-shell support GENERIC_COMMAND_OPTIONS, here we should
      // merge these options to raft properties to make it work.
      final Properties sys = System.getProperties();
      sys.stringPropertyNames().forEach(key -> properties.set(key, sys.getProperty(key)));
    }

    return RaftClient.newBuilder()
        .setRaftGroup(group)
        .setProperties(properties)
        .setParameters(getParameters())
        .setRetryPolicy(getRetryPolicy())
        .build();
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }
}
