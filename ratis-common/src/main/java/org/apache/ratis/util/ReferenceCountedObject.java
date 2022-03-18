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

/**
 * Reference-counted objects.
 *
 * @param <T> The object type.
 */
public interface ReferenceCountedObject<T> {
  /** @return the object. */
  T get();

  /**
   * Retain the object for later use.
   * The reference count will be increased by 1.
   *
   * The {@link #release()} method must be invoked afterward.
   * Otherwise, the object is not returned, and it will cause a resource leak.
   *
   * @return the object.
   */
  T retain();

  /**
   * Release the object.
   * The reference count will be decreased by 1.
   *
   * @return true if the reference count becomes 0; otherwise, return false.
   */
  boolean release();
}
