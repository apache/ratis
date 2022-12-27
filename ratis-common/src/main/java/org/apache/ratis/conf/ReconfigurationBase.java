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

import org.apache.ratis.thirdparty.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.ratis.conf.ReconfigurationUtil.PropertyChange;
import java.util.Optional;

/**
 * Utility base class for implementing the Reconfigurable interface.
 *
 * Subclasses should override reconfigurePropertyImpl to change individual
 * properties and getReconfigurableProperties to get all properties that
 * can be changed at run time.
 */
public abstract class ReconfigurationBase implements Reconfigurable {

  private static final Logger LOG = LoggerFactory.getLogger(ReconfigurationBase.class);

  private ReconfigurationUtil reconfigurationUtil = new ReconfigurationUtil();
  /** Background thread to reload configuration. */
  private Thread reconfigThread = null;
  private volatile boolean shouldRun = true;
  private Object reconfigLock = new Object();
  private RaftProperties prop;
  /**
   * The timestamp when the <code>reconfigThread</code> starts.
   */
  private long startTime = 0;

  /**
   * The timestamp when the <code>reconfigThread</code> finishes.
   */
  private long endTime = 0;


  /**
   * A map of <changed property, error message>. If error message is present,
   * it contains the messages about the error occurred when applies the particular
   * change. Otherwise, it indicates that the change has been successfully applied.
   */
  private Map<PropertyChange, Optional<String>> status = null;

  /**
   * Construct a ReconfigurableBase.
   */
  public ReconfigurationBase() {
    setConf(new RaftProperties());
  }

  /**
   * Construct a ReconfigurableBase with the {@link RaftProperties}
   * @param properties raft properties.
   */
  public ReconfigurationBase(RaftProperties properties) {
    setConf((properties == null) ? new RaftProperties() : properties);
  }

  @Override
  public void setConf(RaftProperties properties) {
    this.prop = properties;
  }

  public RaftProperties getConf() {
    return prop;
  }

  /**
   * Create a new raft properties.
   * @return raftProperties.
   */
  protected abstract RaftProperties getNewConf();

  public Collection<PropertyChange> getChangedProperties(
      RaftProperties newConf, RaftProperties oldConf) {
    return reconfigurationUtil.parseChangedProperties(newConf, oldConf);
  }

  /**
   * A background thread to apply property changes.
   */
  private static class ReconfigurationThread extends Thread {
    private ReconfigurationBase parent;

    ReconfigurationThread(ReconfigurationBase base) {
      this.parent = base;
    }

    // See {@link ReconfigurationServlet#applyChanges}
    public void run() {
      LOG.info("Starting reconfiguration task.");
      final RaftProperties oldConf = parent.getConf();
      final RaftProperties newConf = parent.getNewConf();
      final Collection<PropertyChange> changes =
          parent.getChangedProperties(newConf, oldConf);
      Map<PropertyChange, Optional<String>> results = Maps.newHashMap();
      for (PropertyChange change : changes) {
        String errorMessage = null;
        if (!parent.isPropertyReconfigurable(change.getProp())) {
          LOG.info(String.format(
              "Property %s is not configurable: old value: %s, new value: %s",
              change.getProp(),
              change.getOldVal(),
              change.getNewVal()));
          continue;
        }
        LOG.info("Change property: " + change.getProp() + " from \""
            + ((change.getOldVal() == null) ? "<default>" :  change.getOldVal())
            + "\" to \""
            + ((change.getNewVal() == null) ? "<default>" : change.getNewVal())
            + "\".");
        try {
          String effectiveValue =
              parent.reconfigurePropertyImpl(change.getProp(), change.getNewVal());
          if (change.getNewVal() != null) {
            oldConf.set(change.getProp(), effectiveValue);
          } else {
            oldConf.unset(change.getProp());
          }
        } catch (ReconfigurationException e) {
          Throwable cause = e.getCause();
          errorMessage = cause == null ? e.getMessage() : cause.getMessage();
        }
        results.put(change, Optional.ofNullable(errorMessage));
      }

      synchronized (parent.reconfigLock) {
        parent.endTime = System.nanoTime();
        parent.status = Collections.unmodifiableMap(results);
        parent.reconfigThread = null;
      }
    }
  }

  /**
   * Start a reconfiguration task to reload raft property in background.
   * @throws IOException raised on errors performing I/O.
   */
  public void startReconfigurationTask() throws IOException {
    synchronized (reconfigLock) {
      if (!shouldRun) {
        String errorMessage = "The server is stopped.";
        LOG.warn(errorMessage);
        throw new IOException(errorMessage);
      }
      if (reconfigThread != null) {
        String errorMessage = "Another reconfiguration task is running.";
        LOG.warn(errorMessage);
        throw new IOException(errorMessage);
      }
      reconfigThread = new ReconfigurationThread(this);
      reconfigThread.setDaemon(true);
      reconfigThread.setName("Reconfiguration Task");
      reconfigThread.start();
      startTime = System.nanoTime();
    }
  }

  public ReconfigurationTaskStatus getReconfigurationTaskStatus() {
    synchronized (reconfigLock) {
      if (reconfigThread != null) {
        return new ReconfigurationTaskStatus(startTime, 0, null);
      }
      return new ReconfigurationTaskStatus(startTime, endTime, status);
    }
  }

  public void shutdownReconfigurationTask() {
    Thread tempThread;
    synchronized (reconfigLock) {
      shouldRun = false;
      if (reconfigThread == null) {
        return;
      }
      tempThread = reconfigThread;
      reconfigThread = null;
    }

    try {
      tempThread.join();
    } catch (InterruptedException e) {
    }
  }

  /**
   * {@inheritDoc}
   *
   * This method makes the change to this objects {@link RaftProperties}
   * and calls reconfigurePropertyImpl to update internal data structures.
   * This method cannot be overridden, subclasses should instead override
   * reconfigurePropertyImpl.
   */
  @Override
  public final void reconfigureProperty(String property, String newVal)
      throws ReconfigurationException {
    if (isPropertyReconfigurable(property)) {
      LOG.info("changing property " + property + " to " + newVal);
      synchronized(getConf()) {
        getConf().get(property);
        String effectiveValue = reconfigurePropertyImpl(property, newVal);
        if (newVal != null) {
          getConf().set(property, effectiveValue);
        } else {
          getConf().unset(property);
        }
      }
    } else {
      throw new ReconfigurationException(property, newVal,
          getConf().get(property));
    }
  }

  /**
   * {@inheritDoc}
   *
   * Subclasses must override this.
   */
  @Override
  public abstract Collection<String> getReconfigurableProperties();


  /**
   * {@inheritDoc}
   *
   * Subclasses may wish to override this with a more efficient implementation.
   */
  @Override
  public boolean isPropertyReconfigurable(String property) {
    return getReconfigurableProperties().contains(property);
  }

  /**
   * Change a configuration property.
   *
   * Subclasses must override this. This method applies the change to
   * all internal data structures derived from the configuration property
   * that is being changed. If this object owns other Reconfigurable objects
   * reconfigureProperty should be called recursively to make sure that
   * the configuration of these objects are updated.
   *
   * @param property Name of the property that is being reconfigured.
   * @param newVal Proposed new value of the property.
   * @return Effective new value of the property. This may be different from
   *         newVal.
   *
   * @throws ReconfigurationException if there was an error applying newVal.
   */
  protected abstract String reconfigurePropertyImpl(String property, String newVal)
      throws ReconfigurationException;

}
