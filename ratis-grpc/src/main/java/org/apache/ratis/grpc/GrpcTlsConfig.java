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

import java.io.File;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Ratis GRPC TLS configurations.
 */
public class GrpcTlsConfig extends TlsConf {
  public boolean isFileBasedConfig() {
    return getTrustManager().getTrustCertificates().isFileBased();
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
    return (List<X509Certificate>) getTrustManager().getTrustCertificates().get();
  }

  public File getTrustStoreFile() {
    return getTrustManager().getTrustCertificates().getFile();
  }

  public boolean getMtlsEnabled() {
    return getKeyManager() != null;
  }

  public GrpcTlsConfig(PrivateKey privateKey, X509Certificate certChain,
      List<X509Certificate> trustStore, boolean mTlsEnabled) {
    super(newBuilder(privateKey, certChain, trustStore, mTlsEnabled));
  }

  public GrpcTlsConfig(PrivateKey privateKey, X509Certificate certChain,
      X509Certificate trustStore, boolean mTlsEnabled) {
    this(privateKey, certChain, Collections.singletonList(trustStore), mTlsEnabled);
  }

  public GrpcTlsConfig(File privateKeyFile, File certChainFile,
      File trustStoreFile, boolean mTlsEnabled) {
    super(newBuilder(privateKeyFile, certChainFile, trustStoreFile, mTlsEnabled));
  }

  private static Builder newBuilder(PrivateKey privateKey, X509Certificate certChain,
      List<X509Certificate> trustStore, boolean mTlsEnabled) {
    final CertificatesConf trustManager = new CertificatesConf(trustStore);
    return !mTlsEnabled? newBuilder(trustManager)
        : newBuilder(trustManager, new PrivateKeyConf(privateKey), new CertificatesConf(certChain));
  }

  private static Builder newBuilder(File privateKeyFile, File certChainFile, File trustStoreFile, boolean mTlsEnabled) {
    final CertificatesConf trustManager = new CertificatesConf(trustStoreFile);
    return !mTlsEnabled? newBuilder(trustManager)
        : newBuilder(trustManager, new PrivateKeyConf(privateKeyFile), new CertificatesConf(certChainFile));
  }

  private static Builder newBuilder(CertificatesConf trustManager) {
    return newBuilder().setTrustCertificates(trustManager);
  }

  private static Builder newBuilder(CertificatesConf trustManager,
      PrivateKeyConf privateKey, CertificatesConf certChain) {
    return newBuilder(trustManager).setPrivateKey(privateKey).setKeyCertificates(certChain);
  }
}