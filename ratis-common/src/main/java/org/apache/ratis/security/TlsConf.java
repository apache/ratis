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
package org.apache.ratis.security;

import org.apache.ratis.thirdparty.io.netty.handler.ssl.CipherSuiteFilter;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslProvider;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SupportedCipherSuiteFilter;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;

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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TLS configurations.
 */
public class TlsConf {
  /**
   * The value is either an actual object or a file containing the object.
   * @param <V> The value type.
   */
  private static class FileBasedValue<V> {
    private final V value;
    private final File file;

    FileBasedValue(V value) {
      this.value = Objects.requireNonNull(value, () -> "value == null in " + getClass());
      this.file = null;

      if (value instanceof Iterable) {
        final Iterator<?> i = ((Iterable<?>) value).iterator();
        Preconditions.assertTrue(i.hasNext(), "value is an empty Iterable in " + getClass());
        Objects.requireNonNull(i.next(), () -> "The first item in value is null in " + getClass());
      }
    }

    FileBasedValue(File file) {
      this.value = null;
      this.file = Objects.requireNonNull(file, () -> "file == null in " + getClass());
    }

    public V get() {
      return value;
    }

    public File getFile() {
      return file;
    }

    public final boolean isFileBased() {
      return getFile() != null;
    }
  }

  /** Configuration for {@link X509Certificate}s. */
  public static class CertificatesConf extends FileBasedValue<Iterable<X509Certificate>> {
    public CertificatesConf(Iterable<X509Certificate> certificates) {
      super(certificates);
    }
    public CertificatesConf(X509Certificate... certificates) {
      this(Arrays.asList(certificates));
    }
    public CertificatesConf(File certificates) {
      super(certificates);
    }
  }

  /** Configuration for a {@link PrivateKey}. */
  public static class PrivateKeyConf extends FileBasedValue<PrivateKey> {
    public PrivateKeyConf(PrivateKey privateKey) {
      super(privateKey);
    }
    public PrivateKeyConf(File privateKeyFile) {
      super(privateKeyFile);
    }
  }

  /** Configurations for a trust manager. */
  public static final class TrustManagerConf {
    /** Trust certificates. */
    private final CertificatesConf trustCertificates;
    private final TrustManager trustManager;

    private TrustManagerConf(CertificatesConf trustCertificates) {
      this.trustCertificates = trustCertificates;
      this.trustManager = null;
    }

    private TrustManagerConf(TrustManager trustManager) {
      this.trustManager = trustManager;
      this.trustCertificates = null;
    }

    /** @return the trust certificates. */
    public CertificatesConf getTrustCertificates() {
      return trustCertificates;
    }

    public TrustManager getTrustManager() {
      return trustManager;
    }
  }

  /** Configurations for a key manager. */
  public static final class KeyManagerConf {
    /** A {@link PrivateKey}. */
    private final PrivateKeyConf privateKey;
    /** Certificates for the private key. */
    private final CertificatesConf keyCertificates;
    private final KeyManager keyManager;

    private KeyManagerConf(PrivateKeyConf privateKey, CertificatesConf keyCertificates) {
      this.privateKey = Objects.requireNonNull(privateKey, "privateKey == null");
      this.keyCertificates = Objects.requireNonNull(keyCertificates, "keyCertificates == null");
      Preconditions.assertTrue(privateKey.isFileBased() == keyCertificates.isFileBased(),
          () -> "The privateKey (isFileBased? " + privateKey.isFileBased()
              + ") and the keyCertificates (isFileBased? " + keyCertificates.isFileBased()
              + ") must be either both file based or both not.");
      keyManager = null;
    }

    private KeyManagerConf(KeyManager keyManager) {
      this.keyManager = keyManager;
      this.privateKey = null;
      this.keyCertificates = null;
    }

    /** @return the private key. */
    public PrivateKeyConf getPrivateKey() {
      return privateKey;
    }

    /** @return the certificates for the private key. */
    public CertificatesConf getKeyCertificates() {
      return keyCertificates;
    }

    public boolean isFileBased() {
      return privateKey.isFileBased();
    }

    public KeyManager getKeyManager() {
      return keyManager;
    }
  }

  private static final AtomicInteger COUNT = new AtomicInteger();

  private final String name;
  private final KeyManagerConf keyManager;
  private final TrustManagerConf trustManager;
  private final boolean mutualTls;
  private final SslProvider sslProvider;
  private final String jsseProviderName;
  private final List<String> protocols;
  private final List<String> cipherSuites;
  private final CipherSuiteFilter cipherSuiteFilter;

  protected TlsConf(Builder b) {
    final String buildName = b.buildName();
    this.name = JavaUtils.getClassSimpleName(getClass()) + COUNT.getAndIncrement()
        + (buildName == null? "": "-" + buildName);
    this.keyManager = b.buildKeyManagerConf();
    this.trustManager = b.buildTrustManagerConf();
    this.mutualTls = b.isMutualTls();
    this.sslProvider = b.sslProvider;
    this.jsseProviderName = b.jsseProviderName;
    this.protocols = copy(b.protocols);
    this.cipherSuites = copy(b.cipherSuites);
    this.cipherSuiteFilter = b.cipherSuiteFilter;
  }

  /** @return the key manager configuration. */
  public KeyManagerConf getKeyManager() {
    return keyManager;
  }

  /** @return the trust manager configuration. */
  public TrustManagerConf getTrustManager() {
    return trustManager;
  }

  /** Is mutual TLS enabled? */
  public boolean isMutualTls() {
    return mutualTls;
  }

  public SslProvider getSslProvider() {
    return sslProvider;
  }

  public String getJsseProviderName() {
    return jsseProviderName;
  }

  public List<String> getProtocols() {
    return copy(protocols);
  }

  public List<String> getCipherSuites() {
    return copy(cipherSuites);
  }

  /**
   * @return the filter applied to configured cipher suites. The default is
   *     {@link SupportedCipherSuiteFilter#INSTANCE}.
   */
  public CipherSuiteFilter getCipherSuiteFilter() {
    return cipherSuiteFilter;
  }

  @Override
  public String toString() {
    return name;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private static List<String> copy(List<String> values) {
    return values != null ? Collections.unmodifiableList(new ArrayList<>(values)) : null;
  }

  private static List<String> copy(String[] values) {
    return values != null ? copy(Arrays.asList(values)) : null;
  }

  /** For building {@link TlsConf}. */
  public static class Builder {
    private String name;
    private CertificatesConf trustCertificates;
    private PrivateKeyConf privateKey;
    private CertificatesConf keyCertificates;
    private boolean mutualTls;
    private KeyManager keyManager;
    private TrustManager trustManager;
    private SslProvider sslProvider;
    private String jsseProviderName;
    private List<String> protocols;
    private List<String> cipherSuites;
    private CipherSuiteFilter cipherSuiteFilter = SupportedCipherSuiteFilter.INSTANCE;

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setTrustCertificates(CertificatesConf trustCertificates) {
      this.trustCertificates = trustCertificates;
      return this;
    }

    public Builder setPrivateKey(PrivateKeyConf privateKey) {
      this.privateKey = privateKey;
      return this;
    }

    public Builder setKeyCertificates(CertificatesConf keyCertificates) {
      this.keyCertificates = keyCertificates;
      return this;
    }

    public Builder setKeyManager(KeyManager keyManager) {
      this.keyManager = keyManager;
      return this;
    }

    public Builder setTrustManager(TrustManager trustManager) {
      this.trustManager = trustManager;
      return this;
    }

    public Builder setMutualTls(boolean mutualTls) {
      this.mutualTls = mutualTls;
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

    /**
     * Set the filter applied to configured cipher suites.
     *
     * <p>A pass-through filter such as {@code IdentityCipherSuiteFilter} preserves provider-specific
     * suites that may be absent from Netty's supported set. It does not make an unsupported suite
     * valid; validation may instead fail when the TLS engine is initialized or during the handshake.
     */
    public Builder setCipherSuiteFilter(CipherSuiteFilter cipherSuiteFilter) {
      this.cipherSuiteFilter = Objects.requireNonNull(cipherSuiteFilter, "cipherSuiteFilter == null");
      return this;
    }

    private boolean isMutualTls() {
      return mutualTls;
    }

    private String buildName() {
      return Optional.ofNullable(name).orElse("");
    }

    private TrustManagerConf buildTrustManagerConf() {
      if (trustManager != null) {
        return new TrustManagerConf(trustManager);
      } else {
        return new TrustManagerConf(trustCertificates);
      }
    }

    private KeyManagerConf buildKeyManagerConf() {
      if (keyManager != null) {
        return new KeyManagerConf(keyManager);
      } else if (privateKey == null && keyCertificates == null) {
        return null;
      } else if (privateKey != null && keyCertificates != null) {
        return new KeyManagerConf(privateKey, keyCertificates);
      }
      throw new IllegalStateException("The privateKey (null? " + (privateKey == null)
          + ") and the keyCertificates (null? " + (keyCertificates == null)
          + ") must be either both null or both not.");

    }

    public TlsConf build() {
      return new TlsConf(this);
    }
  }
}
