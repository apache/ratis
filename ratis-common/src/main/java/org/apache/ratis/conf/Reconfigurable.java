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

public interface Reconfigurable {
  /**
   * Change a raft property on this object to the value specified.
   *
   * Change a raft property on this object to the value specified
   * and return the previous value that the raft property was set to
   * (or null if it was not previously set). If newVal is null, set the property
   * to its default value;
   *
   * @param property property name.
   * @param newVal new value.
   * @throws ReconfigurationException if there was an error applying newVal.
   * If the property cannot be changed, throw a
   * {@link ReconfigurationException}.
   */
  void reconfigureProperty(String property, String newVal)
      throws ReconfigurationException;

  /**
   * Return whether a given property is changeable at run time.
   *
   * If isPropertyReconfigurable returns true for a property,
   * then changeConf should not throw an exception when changing
   * this property.
   * @param property property name.
   * @return true if property reconfigurable; false if not.
   */
  boolean isPropertyReconfigurable(String property);

  /**
   * Return all the properties that can be changed at run time.
   * @return reconfigurable properties.
   */
  Collection<String> getReconfigurableProperties();

  /**
   * Set the raft properties to be used by this object.
   * @param prop raft properties to be used
   */
  void setConf(RaftProperties prop);

  /**
   * Return the raft properties used by this object.
   * @return RaftProperties
   */
  RaftProperties getConf();
}
