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

import org.apache.ratis.security.TlsConf.Builder;
import org.apache.ratis.security.TlsConf.CertificatesConf;
import org.apache.ratis.security.TlsConf.PrivateKeyConf;
import org.apache.ratis.util.FileUtils;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.net.URL;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Supplier;

public interface SecurityTestUtils {
  Logger LOG = LoggerFactory.getLogger(SecurityTestUtils.class);

  ClassLoader CLASS_LOADER = SecurityTestUtils.class.getClassLoader();

  static File getResource(String name) {
    final File file = Optional.ofNullable(CLASS_LOADER.getResource(name))
        .map(URL::getFile)
        .map(File::new)
        .orElse(null);
    LOG.info("Getting resource {}: {}", name, file);
    return file;
  }

  static TlsConf newServerTlsConfig(boolean mutualAuthn) {
    LOG.info("newServerTlsConfig: mutualAuthn? {}", mutualAuthn);
    return new Builder()
        .setName("server")
        .setPrivateKey(new PrivateKeyConf(getResource("ssl/server.pem")))
        .setKeyCertificates(new CertificatesConf(getResource("ssl/server.crt")))
        .setTrustCertificates(new CertificatesConf(getResource("ssl/client.crt")))
        .setMutualTls(mutualAuthn)
        .build();
  }

  static TlsConf newClientTlsConfig(boolean mutualAuthn) {
    LOG.info("newClientTlsConfig: mutualAuthn? {}", mutualAuthn);
    return new Builder()
        .setName("client")
        .setPrivateKey(new PrivateKeyConf(getResource("ssl/client.pem")))
        .setKeyCertificates(new CertificatesConf(getResource("ssl/client.crt")))
        .setTrustCertificates(new CertificatesConf(getResource("ssl/ca.crt")))
        .setMutualTls(mutualAuthn)
        .build();
  }

  static PrivateKey getPrivateKey(String keyPath) {
    try {
      File file = getResource(keyPath);
      FileReader keyReader = new FileReader(file);
      PemReader pemReader = new PemReader(keyReader);
      PemObject pemObject = pemReader.readPemObject();
      pemReader.close();
      keyReader.close();

      byte[] content = pemObject.getContent();
      PKCS8EncodedKeySpec privKeySpec = new PKCS8EncodedKeySpec(content);

      KeyFactory keyFactory = KeyFactory.getInstance("RSA");
      return keyFactory.generatePrivate(privKeySpec);
    } catch (Exception e) {
      Assertions.fail("Failed to get private key from " + keyPath + ". Error: "  +
          e.getMessage());
    }
    return null;
  }

  static X509Certificate[] getCertificate(String certPath) {
    try {
      // Read certificates
      X509Certificate[] certificate = new X509Certificate[1];
      CertificateFactory fact = CertificateFactory.getInstance("X.509");
      try (InputStream is = FileUtils.newInputStream(getResource(certPath))) {
        certificate[0] = (X509Certificate) fact.generateCertificate(is);
      }
      return certificate;
    } catch (Exception e) {
      Assertions.fail("Failed to get certificate from " + certPath + ". Error: "  +
          e.getMessage());
    }
    return null;
  }

  static KeyStore getServerKeyStore() {
    try {
      PrivateKey privateKey = getPrivateKey("ssl/server.pem");
      X509Certificate[] certificate = getCertificate("ssl/server.crt");

      // build keyStore
      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(null, null);
      keyStore.setKeyEntry("ratis-server-key", privateKey, new char[0], certificate);
      return keyStore;
    } catch (Exception e) {
      Assertions.fail("Failed to get sever key store " + e.getMessage());
    }
    return null;
  }

  static KeyStore getClientKeyStore() {
    try {
      PrivateKey privateKey = getPrivateKey("ssl/client.pem");
      X509Certificate[] certificate = getCertificate("ssl/client.crt");

      // build keyStore
      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(null, null);
      keyStore.setKeyEntry("ratis-client-key", privateKey, new char[0], certificate);
      return keyStore;
    } catch (Exception e) {
      Assertions.fail("Failed to get client key store " + e.getMessage());
    }
    return null;
  }

  static KeyStore getTrustStore() {
    try {
      X509Certificate[] certificate = getCertificate("ssl/ca.crt");

      // build trustStore
      KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
      trustStore.load(null, null);

      for (X509Certificate cert: certificate) {
        trustStore.setCertificateEntry(cert.getSerialNumber().toString(), cert);
      }
      return trustStore;
    } catch (Exception e) {
      Assertions.fail("Failed to get sever key store " + e.getMessage());
    }
    return null;
  }

  static KeyManager getKeyManager(Supplier<KeyStore> supplier) throws KeyStoreException,
      NoSuchAlgorithmException, UnrecoverableKeyException {
    KeyStore keyStore = supplier.get();

    KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagerFactory.init(keyStore, new char[0]);

    KeyManager[] managers = keyManagerFactory.getKeyManagers();
    return managers[0];
  }

  static X509TrustManager getTrustManager(Supplier<KeyStore> supplier) throws KeyStoreException,
      NoSuchAlgorithmException {
    KeyStore keyStore = supplier.get();
    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
        TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(keyStore);
    TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
    if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
      throw new IllegalStateException("Unexpected default trust managers:"
          + Arrays.toString(trustManagers));
    }
    return (X509TrustManager) trustManagers[0];
  }
}