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

import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.security.Provider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestGrpcTlsConfig {
  @Test
  public void testGrpcTlsConfigCopiesLists() {
    final List<String> protocols = new ArrayList<>(Collections.singletonList("TLSv1.2"));
    final List<String> cipherSuites = new ArrayList<>(Collections.singletonList("TLS_AES_128_GCM_SHA256"));

    final GrpcTlsConfig conf = GrpcTlsConfig.newBuilder()
        .setProtocols(protocols)
        .setCipherSuites(cipherSuites)
        .build();

    protocols.add("TLSv1.3");
    cipherSuites.add("TLS_AES_256_GCM_SHA384");

    Assertions.assertEquals(Collections.singletonList("TLSv1.2"), conf.getProtocols());
    Assertions.assertEquals(Collections.singletonList("TLS_AES_128_GCM_SHA256"), conf.getCipherSuites());
    Assertions.assertThrows(UnsupportedOperationException.class, () -> conf.getProtocols().add("TLSv1.3"));
    Assertions.assertThrows(UnsupportedOperationException.class,
        () -> conf.getCipherSuites().add("TLS_AES_256_GCM_SHA384"));
  }

  @Test
  public void testNewBuilderCopiesGrpcTlsOptions() {
    final GrpcTlsConfig conf = GrpcTlsConfig.newBuilder()
        .setSslProvider(SslProvider.OPENSSL)
        .setJsseProviderName("SunJSSE")
        .setProtocols("TLSv1.2")
        .setCipherSuites("TLS_AES_128_GCM_SHA256")
        .build();

    final GrpcTlsConfig updated = GrpcTlsConfig.newBuilder(conf).build();
    Assertions.assertSame(SslProvider.OPENSSL, updated.getSslProvider());
    Assertions.assertEquals("SunJSSE", updated.getJsseProviderName());
    Assertions.assertEquals(Collections.singletonList("TLSv1.2"), updated.getProtocols());
    Assertions.assertEquals(Arrays.asList("TLS_AES_128_GCM_SHA256"), updated.getCipherSuites());
  }

  @Test
  public void testUnknownJsseProviderUsesGenericJdkConfiguration() {
    final SslContextBuilder builder = GrpcUtil.configureJsseProvider(
        SslContextBuilder.forClient(), new TestProvider());
    Assertions.assertNotNull(builder);
  }

  private static class TestProvider extends Provider {
    private static final long serialVersionUID = 1L;

    TestProvider() {
      super("TestJSSE", 1.0, "Test JSSE provider");
    }
  }
}
