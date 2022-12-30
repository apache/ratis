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

import java.util.Collection;

/**
 * To reconfigure {@link RaftProperties} in runtime.
 */
public interface Reconfigurable {
  /** @return the {@link RaftProperties} to be reconfigured. */
  RaftProperties getProperties();

  /**
   * Change a property on this object to the new value specified.
   * If the new value specified is null, reset the property to its default value.
   * <p>
   * This method must apply the change to all internal data structures derived
   * from the configuration property that is being changed.
   * If this object owns other {@link Reconfigurable} objects,
   * it must call this method recursively in order to update all these objects.
   *
   * @param property the name of the given property.
   * @param newValue the new value.
   * @return the effective value, which could possibly be different from specified new value,
   *         of the property after reconfiguration.
   * @throws ReconfigurationException if the property is not reconfigurable or there is an error applying the new value.
   */
  String reconfigureProperty(String property, String newValue) throws ReconfigurationException;

  /**
   * Is the given property reconfigurable at runtime?
   *
   * @param property the name of the given property.
   * @return true iff the given property is reconfigurable.
   */
  default boolean isPropertyReconfigurable(String property) {
    return getReconfigurableProperties().contains(property);
  }

  /** @return all the properties that are reconfigurable at runtime. */
  Collection<String> getReconfigurableProperties();
}
