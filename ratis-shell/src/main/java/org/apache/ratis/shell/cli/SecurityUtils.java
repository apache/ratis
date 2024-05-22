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
package org.apache.ratis.shell.cli;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.InputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;

public final class SecurityUtils {
  private SecurityUtils() {
    // prevent instantiation
  }

  public static KeyStore getTrustStore()
      throws Exception {
    X509Certificate[] certificate = getCertificate("ssl/ca.crt");

    // build trustStore
    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    trustStore.load(null, null);

    for (X509Certificate cert: certificate) {
      trustStore.setCertificateEntry(cert.getSerialNumber().toString(), cert);
    }
    return trustStore;
  }

  public static X509TrustManager getTrustManager(KeyStore keyStore) throws Exception{
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

  static X509Certificate[] getCertificate(String certPath)
      throws CertificateException, IOException {
    // Read certificates
    X509Certificate[] certificate = new X509Certificate[1];
    CertificateFactory fact = CertificateFactory.getInstance("X.509");
    try (InputStream is = Files.newInputStream(Paths.get(certPath))) {
      certificate[0] = (X509Certificate) fact.generateCertificate(is);
    }
    return certificate;
  }

}
