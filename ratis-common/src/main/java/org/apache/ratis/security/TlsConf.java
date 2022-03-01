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

import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;

import java.io.File;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Iterator;
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

    private TrustManagerConf(CertificatesConf trustCertificates) {
      this.trustCertificates = trustCertificates;
    }

    /** @return the trust certificates. */
    public CertificatesConf getTrustCertificates() {
      return trustCertificates;
    }
  }

  /** Configurations for a key manager. */
  public static final class KeyManagerConf {
    /** A {@link PrivateKey}. */
    private final PrivateKeyConf privateKey;
    /** Certificates for the private key. */
    private final CertificatesConf keyCertificates;

    private KeyManagerConf(PrivateKeyConf privateKey, CertificatesConf keyCertificates) {
      this.privateKey = Objects.requireNonNull(privateKey, "privateKey == null");
      this.keyCertificates = Objects.requireNonNull(keyCertificates, "keyCertificates == null");
    }

    /** @return the private key. */
    public PrivateKeyConf getPrivateKey() {
      return privateKey;
    }

    /** @return the certificates for the private key. */
    public CertificatesConf getKeyCertificates() {
      return keyCertificates;
    }
  }

  private static final AtomicInteger COUNT = new AtomicInteger();

  private final String name;
  private final KeyManagerConf keyManager;
  private final TrustManagerConf trustManager;

  private TlsConf(String name, KeyManagerConf keyManager, TrustManagerConf trustManager) {
    this.name = JavaUtils.getClassSimpleName(getClass()) + COUNT.getAndIncrement() + (name == null? "": "-" + name);
    this.keyManager = keyManager;
    this.trustManager = trustManager;
  }

  protected TlsConf(Builder b) {
    this(b.buildName(), b.buildKeyManagerConf(), b.buildTrustManagerConf());
  }

  /** @return the key manager configuration. */
  public KeyManagerConf getKeyManager() {
    return keyManager;
  }

  /** @return the trust manager configuration. */
  public TrustManagerConf getTrustManager() {
    return trustManager;
  }

  @Override
  public String toString() {
    return name;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /** For building {@link TlsConf}. */
  public static class Builder {
    private String name;
    private CertificatesConf trustCertificates;
    private PrivateKeyConf privateKey;
    private CertificatesConf keyCertificates;

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

    private String buildName() {
      return Optional.ofNullable(name).orElse("");
    }

    private TrustManagerConf buildTrustManagerConf() {
      return new TrustManagerConf(trustCertificates);
    }

    private KeyManagerConf buildKeyManagerConf() {
      if (privateKey == null && keyCertificates == null) {
        return null;
      } else if (privateKey != null && keyCertificates != null) {
        return new KeyManagerConf(privateKey, keyCertificates);
      }
      throw new IllegalStateException("Must be either all null or all non-null: privateKey == null? "
          + (privateKey == null) + ", keyCertificates == null? " + (keyCertificates == null));
    }

    public TlsConf build() {
      return new TlsConf(this);
    }
  }
}