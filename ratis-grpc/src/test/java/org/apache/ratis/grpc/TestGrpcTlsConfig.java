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
package org.apache.ratis.grpc;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGrpcTlsConfig {
  @Test
  public void testRaftPropertiesOverrideGrpcTlsOptions() {
    final GrpcTlsConfig conf = GrpcTlsConfig.newBuilder()
        .setSslProvider(SslProvider.OPENSSL)
        .setProtocols("TLSv1.2")
        .setCipherSuites("TLS_AES_128_GCM_SHA256")
        .build();

    final RaftProperties properties = new RaftProperties();
    GrpcConfigKeys.TLS.setSslProvider(properties, SslProvider.JDK);
    GrpcConfigKeys.TLS.setJsseProviderName(properties, "BCJSSE");
    GrpcConfigKeys.TLS.setProtocols(properties, "TLSv1.3");
    GrpcConfigKeys.TLS.setCipherSuites(properties, "TLS_AES_256_GCM_SHA384");

    final GrpcTlsConfig updated = GrpcConfigKeys.TLS.apply(properties, conf);
    Assertions.assertSame(SslProvider.JDK, updated.getSslProvider());
    Assertions.assertEquals("BCJSSE", updated.getJsseProviderName());
    Assertions.assertArrayEquals(new String[] {"TLSv1.3"}, updated.getProtocols());
    Assertions.assertArrayEquals(new String[] {"TLS_AES_256_GCM_SHA384"}, updated.getCipherSuites());
  }

  @Test
  public void testRaftPropertiesKeepUnsetGrpcTlsOptions() {
    final GrpcTlsConfig conf = GrpcTlsConfig.newBuilder()
        .setSslProvider(SslProvider.OPENSSL)
        .setJsseProviderName("BCJSSE")
        .setProtocols("TLSv1.2")
        .setCipherSuites("TLS_AES_128_GCM_SHA256")
        .build();

    final GrpcTlsConfig updated = GrpcConfigKeys.TLS.apply(new RaftProperties(), conf);
    Assertions.assertSame(SslProvider.OPENSSL, updated.getSslProvider());
    Assertions.assertEquals("BCJSSE", updated.getJsseProviderName());
    Assertions.assertArrayEquals(new String[] {"TLSv1.2"}, updated.getProtocols());
    Assertions.assertArrayEquals(new String[] {"TLS_AES_128_GCM_SHA256"}, updated.getCipherSuites());
  }
}
