/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.ratis.server.storage;

import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.AtomicFileOutputStream;
import org.apache.ratis.util.ConcurrentUtils;
import org.apache.ratis.util.JavaUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represent a file on disk which persistently stores the metadata of a raft storage.
 * The file is updated atomically.
 */
class RaftStorageMetadataFileImpl implements RaftStorageMetadataFile {
  private static final String TERM_KEY = "term";
  private static final String VOTED_FOR_KEY = "votedFor";

  private final File file;
  private final AtomicReference<RaftStorageMetadata> metadata = new AtomicReference<>();

  RaftStorageMetadataFileImpl(File file) {
    this.file = file;
  }

  @Override
  public RaftStorageMetadata getMetadata() throws IOException {
    return ConcurrentUtils.updateAndGet(metadata, value -> value != null? value: load(file));
  }

  @Override
  public void persist(RaftStorageMetadata newMetadata) throws IOException {
    ConcurrentUtils.updateAndGet(metadata,
        old -> Objects.equals(old, newMetadata)? old: atomicWrite(newMetadata, file));
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(RaftStorageMetadataFile.class) + ":" + file;
  }

  /**
   * Atomically write the given term and votedFor information to the given file,
   * including fsyncing.
   *
   * @throws IOException if the file cannot be written
   */
  static RaftStorageMetadata atomicWrite(RaftStorageMetadata metadata, File file) throws IOException {
    final Properties properties = new Properties();
    properties.setProperty(TERM_KEY, Long.toString(metadata.getTerm()));
    properties.setProperty(VOTED_FOR_KEY, metadata.getVotedFor().toString());

    try(BufferedWriter out = new BufferedWriter(
        new OutputStreamWriter(new AtomicFileOutputStream(file), StandardCharsets.UTF_8))) {
      properties.store(out, "");
    }
    return metadata;
  }

  static Object getValue(String key, Properties properties) throws IOException {
    return Optional.ofNullable(properties.getProperty(key)).orElseThrow(
        () -> new IOException("'" + key + "' not found in properties: " + properties));
  }

  static long getTerm(Properties properties) throws IOException {
    try {
      return Long.parseLong((String) getValue(TERM_KEY, properties));
    } catch (Exception e) {
      throw new IOException("Failed to parse '" + TERM_KEY + "' from properties: " + properties, e);
    }
  }

  static RaftPeerId getVotedFor(Properties properties) throws IOException {
    try {
      return RaftPeerId.valueOf((String) getValue(VOTED_FOR_KEY, properties));
    } catch (Exception e) {
      throw new IOException("Failed to parse '" + VOTED_FOR_KEY + "' from properties: " + properties, e);
    }
  }

  static RaftStorageMetadata load(File file) throws IOException {
    if (!file.exists()) {
      return RaftStorageMetadata.getDefault();
    }
    try(BufferedReader br = new BufferedReader(new InputStreamReader(
        new FileInputStream(file), StandardCharsets.UTF_8))) {
      Properties properties = new Properties();
      properties.load(br);
      return RaftStorageMetadata.valueOf(getTerm(properties), getVotedFor(properties));
    } catch (IOException e) {
      throw new IOException("Failed to load " + file, e);
    }
  }
}
