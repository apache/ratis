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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.Optional;

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
}