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

package org.apache.ratis.conf;

import org.apache.ratis.conf.ReconfigurationStatus.PropertyChange;
import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for implementing the {@link Reconfigurable} interface.
 * Subclasses must override
 * (1) {@link #getReconfigurableProperties()} to return all properties that can be reconfigurable at runtime,
 * (2) {@link #getNewProperties()} to return the new {@link RaftProperties} to be reconfigured to, and
 * (3) {@link #reconfigureProperty(String, String)} to change individual properties.
 */
public abstract class ReconfigurationBase implements Reconfigurable {
  private static final Logger LOG = LoggerFactory.getLogger(ReconfigurationBase.class);

  public static Collection<PropertyChange> getChangedProperties(
      RaftProperties newProperties, RaftProperties oldProperties) {
    final Map<String, PropertyChange> changes = new HashMap<>();

    // iterate over old properties
    for (Map.Entry<String, String> oldEntry: oldProperties.entrySet()) {
      final String prop = oldEntry.getKey();
      final String oldVal = oldEntry.getValue();
      final String newVal = newProperties.getRaw(prop);

      if (!Objects.equals(newVal, oldVal)) {
        changes.put(prop, new PropertyChange(prop, newVal, oldVal));
      }
    }

    // now iterate over new properties in order to look for properties not present in old properties
    for (Map.Entry<String, String> newEntry: newProperties.entrySet()) {
      final String prop = newEntry.getKey();
      final String newVal = newEntry.getValue();
      if (newVal != null && oldProperties.get(prop) == null) {
        changes.put(prop, new PropertyChange(prop, newVal, null));
      }
    }

    return changes.values();
  }

  class Context {
    /** The current reconfiguration status. */
    private ReconfigurationStatus status = new ReconfigurationStatus(null, null, null, null);
    /** Is this context stopped? */
    private boolean isStopped;

    synchronized ReconfigurationStatus getStatus() {
      return status;
    }

    synchronized void start() {
      if (isStopped) {
        throw new IllegalStateException(name + " is stopped.");
      }
      final Daemon previous = status.getDaemon();
      if (previous != null) {
        throw new IllegalStateException(name + ": a reconfiguration task " + previous + " is already running.");
      }
      final Timestamp startTime = Timestamp.currentTime();
      final Daemon task = Daemon.newBuilder()
          .setName("started@" + startTime)
          .setRunnable(ReconfigurationBase.this::batchReconfiguration)
          .build();
      status = new ReconfigurationStatus(startTime, null, null, task);
      task.start();
    }

    synchronized void end(Map<PropertyChange, Throwable> results) {
      status = new ReconfigurationStatus(status.getStartTime(), Timestamp.currentTime(), results, null);
    }

    synchronized Daemon stop() {
      isStopped = true;
      final Daemon task = status.getDaemon();
      status = new ReconfigurationStatus(status.getStartTime(), null, null, null);
      return task;
    }
  }

  private final String name;
  private final RaftProperties properties;
  private final Context context;

  /**
   * Construct a ReconfigurableBase with the {@link RaftProperties}
   * @param properties raft properties.
   */
  protected ReconfigurationBase(String name, RaftProperties properties) {
    this.name = name;
    this.properties = properties;
    this.context = new Context();
  }

  @Override
  public RaftProperties getProperties() {
    return properties;
  }

  /** @return the new {@link RaftProperties} to be reconfigured to. */
  protected abstract RaftProperties getNewProperties();

  /**
   * Start a reconfiguration task to reload raft property in background.
   * @throws IOException raised on errors performing I/O.
   */
  public void startReconfiguration() throws IOException {
    context.start();
  }

  public ReconfigurationStatus getReconfigurationStatus() {
    return context.getStatus();
  }

  public void shutdown() throws InterruptedException {
    context.stop().join();
  }

  /**
   * Run a batch reconfiguration to change the current properties
   * to the properties returned by {@link #getNewProperties()}.
   */
  private void batchReconfiguration() {
    LOG.info("{}: Starting batch reconfiguration {}", name, Thread.currentThread());
    final Collection<PropertyChange> changes = getChangedProperties(getNewProperties(), properties);
    final Map<PropertyChange, Throwable> results = new HashMap<>();
    for (PropertyChange change : changes) {
      LOG.info("Change property: " + change);
      try {
        singleReconfiguration(change.getProperty(), change.getNewValue());
        results.put(change, null);
      } catch (Throwable t) {
        results.put(change, t);
      }
    }
    context.end(results);
  }

  /** Run a single reconfiguration to change the given property to the given value. */
  private void singleReconfiguration(String property, String newValue) throws ReconfigurationException {
    if (!isPropertyReconfigurable(property)) {
      throw new ReconfigurationException("Property is not reconfigurable.",
          property, newValue, properties.get(property));
    }
    final String effective = reconfigureProperty(property, newValue);
    LOG.info("{}: changed property {} to {} (effective {})", name, property, newValue, effective);
    if (newValue != null) {
      properties.set(property, effective);
    } else {
      properties.unset(property);
    }
  }
}