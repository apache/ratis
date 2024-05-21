package org.apache.ratis.shell.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Supplier;

public class SecurityUtils {
  static Logger LOG = LoggerFactory.getLogger(SecurityUtils.class);

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
