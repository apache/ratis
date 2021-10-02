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
package org.apache.ratis.ratisshell;

import java.util.function.Supplier;

/**
 * Supplier for a configuration property default.
 */
public class DefaultSupplier implements Supplier<Object> {
  private final Supplier<Object> mSupplier;
  private final String mDescription;

  /**
   * @param supplier the value
   * @param description a description of the default value
   */
  public DefaultSupplier(Supplier<Object> supplier, String description) {
    mSupplier = supplier;
    mDescription = description;
  }

  @Override
  public Object get() {
    return mSupplier.get();
  }

  /**
   * @return a description of how the default value is determined
   */
  public String getDescription() {
    return mDescription;
  }
}
