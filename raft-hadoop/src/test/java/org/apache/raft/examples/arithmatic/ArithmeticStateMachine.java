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
package org.apache.raft.examples.arithmatic;

import org.apache.raft.conf.RaftProperties;
import org.apache.raft.examples.arithmatic.expression.Expression;
import org.apache.raft.proto.RaftProtos.LogEntryProto;
import org.apache.raft.protocol.Message;
import org.apache.raft.server.RaftConfiguration;
import org.apache.raft.server.RaftServerConstants;
import org.apache.raft.server.StateMachine;
import org.apache.raft.server.storage.RaftStorage;
import org.apache.raft.server.storage.RaftStorageDirectory;
import org.apache.raft.util.AutoCloseableLock;
import org.apache.raft.util.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ArithmeticStateMachine implements StateMachine {
  static final Logger LOG = LoggerFactory.getLogger(ArithmeticStateMachine.class);

  private RaftConfiguration conf;
  private final Map<String, Double> variables = new ConcurrentHashMap<>();
  private long lastAppliedIndex = RaftServerConstants.INVALID_LOG_INDEX;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  private AutoCloseableLock readLock() {
    return AutoCloseableLock.acquire(lock.readLock());
  }

  private AutoCloseableLock writeLock() {
    return AutoCloseableLock.acquire(lock.writeLock());
  }

  void reset() {
    variables.clear();
    lastAppliedIndex = RaftServerConstants.INVALID_LOG_INDEX;
  }

  @Override
  public void initialize(RaftProperties properties, RaftStorage storage) {
  }

  @Override
  public long takeSnapshot(File snapshotFile, RaftStorage storage) {
    final Map<String, Double> copy;
    final long last;
    try(final AutoCloseableLock readLock = readLock()) {
      copy = new HashMap<>(variables);
      last = lastAppliedIndex;
    }

    try(final ObjectOutputStream out = new ObjectOutputStream(
        new BufferedOutputStream(new FileOutputStream(snapshotFile)))) {
      out.writeObject(copy);
    } catch(IOException ioe) {
      LOG.warn("Failed to write snapshot file \"" + snapshotFile
          + "\", last applied index=" + last);
    }

    return last;
  }

  @Override
  public long loadSnapshot(File snapshotFile) throws IOException {
    return load(snapshotFile, false);
  }

  @Override
  public long reloadSnapshot(File snapshotFile) throws IOException {
    return load(snapshotFile, true);
  }

  private long load(File snapshotFile, boolean reload) throws IOException {
    if (snapshotFile == null || !snapshotFile.exists()) {
      LOG.warn("The snapshot file {} does not exist", snapshotFile);
      return RaftServerConstants.INVALID_LOG_INDEX;
    }

    final long last = RaftStorageDirectory.getIndexFromSnapshotFile(snapshotFile);
    try(final AutoCloseableLock writeLock = writeLock();
        final ObjectInputStream in = new ObjectInputStream(
            new BufferedInputStream(new FileInputStream(snapshotFile)))) {
      if (reload) {
        reset();
      }
      lastAppliedIndex = last;
      variables.putAll((Map<String, Double>) in.readObject());
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
    return last;
  }

  @Override
  public void setRaftConfiguration(RaftConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public RaftConfiguration getRaftConfiguration() {
    return conf;
  }

  @Override
  public void close() {
    reset();
  }

  @Override
  public Message applyLogEntry(LogEntryProto entry) {
    final Message message = ProtoUtils.toMessage(entry.getClientMessageEntry());
    final AssignmentMessage assignment = new AssignmentMessage(message);

    final long last = entry.getIndex();
    final Double result;
    try(final AutoCloseableLock writeLock = writeLock()) {
      result = assignment.evaluate(variables);
      lastAppliedIndex = last;
    }
    final Expression r = Expression.Utils.double2Expression(result);
    LOG.debug("{}: {} = {}, variables={}", last, assignment, r, variables);
    return Expression.Utils.toMessage(r);
  }

  @Override
  public Message query(Message query) {
    final Expression q = Expression.Utils.bytes2Expression(query.getContent(), 0);
    final Double result;
    try(final AutoCloseableLock readLock = readLock()) {
      result = q.evaluate(variables);
    }
    final Expression r = Expression.Utils.double2Expression(result);
    LOG.debug("QUERY: {} = {}", q, r);
    return Expression.Utils.toMessage(r);
  }
}
