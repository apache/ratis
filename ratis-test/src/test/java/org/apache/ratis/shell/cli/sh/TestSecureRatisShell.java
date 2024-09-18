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
package org.apache.ratis.shell.cli.sh;

import org.apache.ratis.BaseTest;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.netty.NettyUtils;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.security.SecurityTestUtils;
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;

public class TestSecureRatisShell extends BaseTest {
  {
    Slf4jUtils.setLogLevel(NettyUtils.LOG, Level.DEBUG);
  }

  private static final Parameters SERVER_PARAMETERS = new Parameters();
  private static final Parameters CLIENT_PARAMETERS = new Parameters();

  static {
    final TrustManager emptyTrustManager = SecurityTestUtils.emptyTrustManager();
    try {
      final KeyManager serverKeyManager = SecurityTestUtils.getKeyManager(SecurityTestUtils::getServerKeyStore);
      final GrpcTlsConfig serverConfig = new GrpcTlsConfig(serverKeyManager, emptyTrustManager, true);
      GrpcConfigKeys.Server.setTlsConf(SERVER_PARAMETERS, serverConfig);
      GrpcConfigKeys.Admin.setTlsConf(SERVER_PARAMETERS, serverConfig);
      GrpcConfigKeys.Client.setTlsConf(SERVER_PARAMETERS, serverConfig);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to init SERVER_PARAMETERS", e);
    }

    try {
      final KeyManager clientKeyManager = SecurityTestUtils.getKeyManager(SecurityTestUtils::getClientKeyStore);
      final GrpcTlsConfig clientConfig = new GrpcTlsConfig(clientKeyManager, emptyTrustManager, true);
      GrpcConfigKeys.Admin.setTlsConf(CLIENT_PARAMETERS, clientConfig);
      GrpcConfigKeys.Client.setTlsConf(CLIENT_PARAMETERS, clientConfig);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to init CLIENT_PARAMETERS", e);
    }
  }

  @Test
  public void testRatisShell() throws Exception {
    final String[] ids = {"s0"};
    final RaftProperties properties = new RaftProperties();
    RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.ONE_MINUTE);
    GrpcConfigKeys.TLS.setEnabled(properties, true);
    GrpcConfigKeys.TLS.setMutualAuthnEnabled(properties, true);

    try(MiniRaftClusterWithGrpc cluster = new MiniRaftClusterWithGrpc(ids, properties, SERVER_PARAMETERS)) {
      cluster.start();

      runTestRatisShell(cluster, true);
      runTestRatisShell(cluster, false);
    }
  }

  void runTestRatisShell(MiniRaftClusterWithGrpc cluster, boolean secure) throws Exception {
    try(ByteArrayOutputStream out = new ByteArrayOutputStream(1 << 16);
        RatisShell shell = newRatisShell(out, cluster.getProperties(), secure)) {
      shell.run("group", "info", "-peers", toCliArg(cluster.getPeers()));
      final String output = out.toString();
      LOG.info("output (secure? {}):\n{}", secure, output);
      final String gid = cluster.getGroup().getGroupId().getUuid().toString();
      if (secure) {
        Assertions.assertTrue(output.contains(gid), () -> gid + " not found for secure shell");
      } else {
        Assertions.assertTrue(output.contains("Failed to get group ID"), "Unexpected output for unsecure shell");
      }
    }
  }

  static RatisShell newRatisShell(OutputStream out, RaftProperties properties, boolean secure) {
    final PrintStream printStream = new PrintStream(out, true);
    if (!secure) {
      return new RatisShell(printStream);
    }
    return RatisShell.newBuilder()
        .setPrintStream(printStream)
        .setProperties(properties)
        .setParameters(CLIENT_PARAMETERS)
        .build();
  }

  static String toCliArg(List<RaftPeer> peers) {
    final StringBuilder b = new StringBuilder();
    for(RaftPeer peer : peers) {
      b.append(peer.getAdminAddress()).append(",");
    }
    return b.substring(0, b.length() - 1);
  }
}
