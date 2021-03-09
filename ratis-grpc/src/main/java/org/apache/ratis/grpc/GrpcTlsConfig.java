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

import java.io.File;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;

/**
 * Ratis GRPC TLS configurations.
 */
public class GrpcTlsConfig {
  // private key
  private PrivateKey privateKey;
  private File privateKeyFile;

  // certificate
  private X509Certificate certChain;
  private File certChainFile;

  // ca certificate
  private List<X509Certificate> trustStore;
  private File trustStoreFile;

  // mutual TLS enabled
  private final boolean mTlsEnabled;

  private final boolean fileBasedConfig;

  public boolean isFileBasedConfig() {
    return fileBasedConfig;
  }

  public PrivateKey getPrivateKey() {
    return privateKey;
  }

  public File getPrivateKeyFile() {
    return privateKeyFile;
  }

  public X509Certificate getCertChain() {
    return certChain;
  }

  public File getCertChainFile() {
    return certChainFile;
  }

  public List<X509Certificate> getTrustStore() {
    return trustStore;
  }

  public File getTrustStoreFile() {
    return trustStoreFile;
  }

  public boolean getMtlsEnabled() {
    return mTlsEnabled;
  }

  public GrpcTlsConfig(PrivateKey privateKey, X509Certificate certChain,
      List<X509Certificate> trustStore, boolean mTlsEnabled) {
    this.privateKey = privateKey;
    this.certChain = certChain;
    this.trustStore = trustStore;
    this.mTlsEnabled = mTlsEnabled;
    this.fileBasedConfig = false;
  }

  public GrpcTlsConfig(PrivateKey privateKey, X509Certificate certChain,
      X509Certificate trustStore, boolean mTlsEnabled) {
    this.privateKey = privateKey;
    this.certChain = certChain;
    this.trustStore = Collections.singletonList(trustStore);
    this.mTlsEnabled = mTlsEnabled;
    this.fileBasedConfig = false;
  }

  public GrpcTlsConfig(File privateKeyFile, File certChainFile,
      File trustStoreFile, boolean mTlsEnabled) {
    this.privateKeyFile = privateKeyFile;
    this.certChainFile = certChainFile;
    this.trustStoreFile = trustStoreFile;
    this.mTlsEnabled = mTlsEnabled;
    this.fileBasedConfig = true;
  }
}