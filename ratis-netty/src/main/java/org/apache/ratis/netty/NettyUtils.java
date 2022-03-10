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
package org.apache.ratis.netty;

import org.apache.ratis.security.TlsConf;
import org.apache.ratis.security.TlsConf.CertificatesConf;
import org.apache.ratis.security.TlsConf.KeyManagerConf;
import org.apache.ratis.security.TlsConf.PrivateKeyConf;
import org.apache.ratis.security.TlsConf.TrustManagerConf;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.epoll.Epoll;
import org.apache.ratis.thirdparty.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContext;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.apache.ratis.util.ConcurrentUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public interface NettyUtils {
  Logger LOG = LoggerFactory.getLogger(NettyUtils.class);

  static EventLoopGroup newEventLoopGroup(String name, int size, boolean useEpoll) {
    if (useEpoll) {
      if (Epoll.isAvailable()) {
        LOG.info("Create EpollEventLoopGroup for {}; Thread size is {}.", name, size);
        return new EpollEventLoopGroup(size, ConcurrentUtils.newThreadFactory(name + "-"));
      } else {
        LOG.warn("Failed to create EpollEventLoopGroup for " + name + "; fall back on NioEventLoopGroup.",
            Epoll.unavailabilityCause());
      }
    }
    return new NioEventLoopGroup(size, ConcurrentUtils.newThreadFactory(name + "-"));
  }

  static void setTrustManager(SslContextBuilder b, TrustManagerConf trustManagerConfig) {
    if (trustManagerConfig == null) {
      return;
    }
    final CertificatesConf certificates = trustManagerConfig.getTrustCertificates();
    if (certificates.isFileBased()) {
      b.trustManager(certificates.getFile());
    } else {
      b.trustManager(certificates.get());
    }
  }

  static void setKeyManager(SslContextBuilder b, KeyManagerConf keyManagerConfig) {
    if (keyManagerConfig == null) {
      return;
    }
    final PrivateKeyConf privateKey = keyManagerConfig.getPrivateKey();
    final CertificatesConf certificates = keyManagerConfig.getKeyCertificates();

    if (keyManagerConfig.isFileBased()) {
      b.keyManager(certificates.getFile(), privateKey.getFile());
    } else {
      b.keyManager(privateKey.get(), certificates.get());
    }
  }

  static SslContextBuilder initSslContextBuilderForServer(KeyManagerConf keyManagerConfig) {
    final PrivateKeyConf privateKey = keyManagerConfig.getPrivateKey();
    final CertificatesConf certificates = keyManagerConfig.getKeyCertificates();

    if (keyManagerConfig.isFileBased()) {
      return SslContextBuilder.forServer(certificates.getFile(), privateKey.getFile());
    } else {
      return SslContextBuilder.forServer(privateKey.get(), certificates.get());
    }
  }

  static SslContextBuilder initSslContextBuilderForServer(TlsConf tlsConf) {
    final SslContextBuilder b = initSslContextBuilderForServer(tlsConf.getKeyManager());
    if (tlsConf.isMutualTls()) {
      setTrustManager(b, tlsConf.getTrustManager());
    }
    return b;
  }

  static SslContext buildSslContextForServer(TlsConf tlsConf) {
    return buildSslContext(tlsConf, true, NettyUtils::initSslContextBuilderForServer);
  }

  static SslContextBuilder initSslContextBuilderForClient(TlsConf tlsConf) {
    final SslContextBuilder b = SslContextBuilder.forClient();
    setTrustManager(b, tlsConf.getTrustManager());
    if (tlsConf.isMutualTls()) {
      setKeyManager(b, tlsConf.getKeyManager());
    }
    return b;
  }

  static SslContext buildSslContextForClient(TlsConf tlsConf) {
    return buildSslContext(tlsConf, false, NettyUtils::initSslContextBuilderForClient);
  }

  static SslContext buildSslContext(TlsConf tlsConf, boolean isServer, Function<TlsConf, SslContextBuilder> builder) {
    if (tlsConf == null) {
      return null;
    }
    try {
      return builder.apply(tlsConf).build();
    } catch (Exception e) {
      final String message = "Failed to build a " + (isServer ? "server" : "client") + " SslContext from " + tlsConf;
      throw new IllegalArgumentException(message, e);
    }
  }
}