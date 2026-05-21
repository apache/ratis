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
package org.apache.ratis.grpc;

import org.apache.ratis.security.TlsConf;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslProvider;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import java.io.File;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Ratis GRPC TLS configurations.
 */
public class GrpcTlsConfig extends TlsConf {
  private final boolean fileBasedConfig;
  private final SslProvider sslProvider;
  private final String jsseProviderName;
  private final List<String> protocols;
  private final List<String> cipherSuites;

  public boolean isFileBasedConfig() {
    return fileBasedConfig;
  }

  public PrivateKey getPrivateKey() {
    return Optional.ofNullable(getKeyManager())
        .map(KeyManagerConf::getPrivateKey)
        .map(PrivateKeyConf::get)
        .orElse(null);
  }

  public File getPrivateKeyFile() {
    return Optional.ofNullable(getKeyManager())
        .map(KeyManagerConf::getPrivateKey)
        .map(PrivateKeyConf::getFile)
        .orElse(null);
  }

  public X509Certificate getCertChain() {
    return Optional.ofNullable(getKeyManager())
        .map(KeyManagerConf::getKeyCertificates)
        .map(CertificatesConf::get)
        .map(Iterable::iterator)
        .map(Iterator::next)
        .orElse(null);
  }

  public File getCertChainFile() {
    return Optional.ofNullable(getKeyManager())
        .map(KeyManagerConf::getKeyCertificates)
        .map(CertificatesConf::getFile)
        .orElse(null);
  }

  public List<X509Certificate> getTrustStore() {
    return (List<X509Certificate>) Optional.ofNullable(getTrustManager())
        .map(TrustManagerConf::getTrustCertificates)
        .map(CertificatesConf::get)
        .orElse(null);
  }

  public File getTrustStoreFile() {
    return Optional.ofNullable(getTrustManager())
        .map(TrustManagerConf::getTrustCertificates)
        .map(CertificatesConf::getFile)
        .orElse(null);
  }

  public boolean getMtlsEnabled() {
    return isMutualTls();
  }

  public SslProvider getSslProvider() {
    return sslProvider;
  }

  public String getJsseProviderName() {
    return jsseProviderName;
  }

  public List<String> getProtocols() {
    return protocols;
  }

  public List<String> getCipherSuites() {
    return cipherSuites;
  }

  public GrpcTlsConfig(PrivateKey privateKey, X509Certificate certChain,
      List<X509Certificate> trustStore, boolean mTlsEnabled) {
    this(newBuilder(privateKey, certChain, trustStore, mTlsEnabled), false);
  }

  public GrpcTlsConfig(PrivateKey privateKey, X509Certificate certChain,
      X509Certificate trustStore, boolean mTlsEnabled) {
    this(privateKey, certChain, Collections.singletonList(trustStore), mTlsEnabled);
  }

  public GrpcTlsConfig(File privateKeyFile, File certChainFile,
      File trustStoreFile, boolean mTlsEnabled) {
    this(newBuilder(privateKeyFile, certChainFile, trustStoreFile, mTlsEnabled), true);
  }

  private GrpcTlsConfig(Builder builder, boolean fileBasedConfig) {
    super(builder);
    this.fileBasedConfig = fileBasedConfig;
    this.sslProvider = builder.sslProvider;
    this.jsseProviderName = builder.jsseProviderName;
    this.protocols = copy(builder.protocols);
    this.cipherSuites = copy(builder.cipherSuites);
  }

  public GrpcTlsConfig(KeyManager keyManager, TrustManager trustManager, boolean mTlsEnabled) {
    this(newBuilder(keyManager, trustManager, mTlsEnabled), false);
  }

  private static Builder newBuilder(PrivateKey privateKey, X509Certificate certChain,
      List<X509Certificate> trustStore, boolean mTlsEnabled) {
    final Builder b = newBuilder().setMutualTls(mTlsEnabled);
    Optional.ofNullable(trustStore).map(CertificatesConf::new).ifPresent(b::setTrustCertificates);
    Optional.ofNullable(privateKey).map(PrivateKeyConf::new).ifPresent(b::setPrivateKey);
    Optional.ofNullable(certChain).map(CertificatesConf::new).ifPresent(b::setKeyCertificates);
    return b;
  }

  private static Builder newBuilder(File privateKeyFile, File certChainFile, File trustStoreFile, boolean mTlsEnabled) {
    final Builder b = newBuilder().setMutualTls(mTlsEnabled);
    Optional.ofNullable(trustStoreFile).map(CertificatesConf::new).ifPresent(b::setTrustCertificates);
    Optional.ofNullable(privateKeyFile).map(PrivateKeyConf::new).ifPresent(b::setPrivateKey);
    Optional.ofNullable(certChainFile).map(CertificatesConf::new).ifPresent(b::setKeyCertificates);
    return b;
  }

  private static Builder newBuilder(KeyManager keyManager, TrustManager trustManager, boolean mTlsEnabled) {
    return newBuilder().setMutualTls(mTlsEnabled).setKeyManager(keyManager).setTrustManager(trustManager);
  }

  public static Builder newBuilder(GrpcTlsConfig conf) {
    final Builder b = newBuilder().setMutualTls(conf.isMutualTls());
    Optional.ofNullable(conf.getKeyManager()).ifPresent(keyManager -> {
      if (keyManager.getKeyManager() != null) {
        b.setKeyManager(keyManager.getKeyManager());
      } else {
        b.setPrivateKey(keyManager.getPrivateKey());
        b.setKeyCertificates(keyManager.getKeyCertificates());
      }
    });
    Optional.ofNullable(conf.getTrustManager()).ifPresent(trustManager -> {
      if (trustManager.getTrustManager() != null) {
        b.setTrustManager(trustManager.getTrustManager());
      } else {
        b.setTrustCertificates(trustManager.getTrustCertificates());
      }
    });
    return b.setSslProvider(conf.getSslProvider())
        .setJsseProviderName(conf.getJsseProviderName())
        .setProtocols(conf.getProtocols())
        .setCipherSuites(conf.getCipherSuites());
  }

  private static List<String> copy(List<String> values) {
    return values != null ? Collections.unmodifiableList(new ArrayList<>(values)) : null;
  }

  private static List<String> copy(String[] values) {
    return values != null ? copy(Arrays.asList(values)) : null;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /** For building {@link GrpcTlsConfig}. */
  public static class Builder extends TlsConf.Builder {
    private SslProvider sslProvider;
    private String jsseProviderName;
    private List<String> protocols;
    private List<String> cipherSuites;

    @Override
    public Builder setName(String name) {
      super.setName(name);
      return this;
    }

    @Override
    public Builder setTrustCertificates(CertificatesConf trustCertificates) {
      super.setTrustCertificates(trustCertificates);
      return this;
    }

    @Override
    public Builder setPrivateKey(PrivateKeyConf privateKey) {
      super.setPrivateKey(privateKey);
      return this;
    }

    @Override
    public Builder setKeyCertificates(CertificatesConf keyCertificates) {
      super.setKeyCertificates(keyCertificates);
      return this;
    }

    @Override
    public Builder setKeyManager(KeyManager keyManager) {
      super.setKeyManager(keyManager);
      return this;
    }

    @Override
    public Builder setTrustManager(TrustManager trustManager) {
      super.setTrustManager(trustManager);
      return this;
    }

    @Override
    public Builder setMutualTls(boolean mutualTls) {
      super.setMutualTls(mutualTls);
      return this;
    }

    public Builder setSslProvider(SslProvider sslProvider) {
      this.sslProvider = sslProvider;
      return this;
    }

    public Builder setJsseProviderName(String jsseProviderName) {
      this.jsseProviderName = jsseProviderName;
      return this;
    }

    public Builder setProtocols(String... protocols) {
      this.protocols = copy(protocols);
      return this;
    }

    public Builder setProtocols(List<String> protocols) {
      this.protocols = copy(protocols);
      return this;
    }

    public Builder setCipherSuites(String... cipherSuites) {
      this.cipherSuites = copy(cipherSuites);
      return this;
    }

    public Builder setCipherSuites(List<String> cipherSuites) {
      this.cipherSuites = copy(cipherSuites);
      return this;
    }

    @Override
    public GrpcTlsConfig build() {
      return new GrpcTlsConfig(this, false);
    }
  }
}
