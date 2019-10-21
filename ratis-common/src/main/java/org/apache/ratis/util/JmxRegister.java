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
package org.apache.ratis.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.function.Supplier;

/** For registering JMX beans. */
public class JmxRegister {
  static final Logger LOG = LoggerFactory.getLogger(JmxRegister.class);

  private ObjectName registeredName;

  static ObjectName tryRegister(String name, Object mBean) {
    final ObjectName objectName;
    try {
      objectName = new ObjectName(name);
      ManagementFactory.getPlatformMBeanServer().registerMBean(mBean, objectName);
    } catch (Exception e) {
      LOG.error("Failed to register JMX Bean with name " + name, e);
      return null;
    }

    LOG.info("Successfully registered JMX Bean with object name " + objectName);
    return objectName;
  }

  /**
   * Try registering the mBean with the names one by one.
   * @return the registered name, or, if it fails, return null.
   */
  public synchronized String register(Object mBean, Iterable<Supplier<String>> names) {
    if (registeredName == null) {
      for (Supplier<String> supplier : names) {
        final String name = supplier.get();
        registeredName = tryRegister(name, mBean);
        if (registeredName != null) {
          return name;
        }
      }
    }

    // failed
    return null;
  }

  /** Un-register the previously registered mBean. */
  public synchronized boolean unregister() throws JMException {
    if (registeredName == null) {
      return false;
    }
    ManagementFactory.getPlatformMBeanServer().unregisterMBean(registeredName);
    LOG.info("Successfully un-registered JMX Bean with object name " + registeredName);
    registeredName = null;
    return true;
  }
}
