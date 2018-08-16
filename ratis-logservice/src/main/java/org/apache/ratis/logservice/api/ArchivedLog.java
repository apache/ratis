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
package org.apache.ratis.logservice.api;

/**
 * A {@link LogStream} which has been archived in some external
 * system. This interface is parameterized to allow for implementations
 * to use their own class to encapsulate how to find the archived log.
 *
 * In the majority of cases, this should be transparent to end-users, as
 * the {@link LogStream} should hide the fact that this even exists.
 * TODO maybe that means this should be client-facing at all?
 *
 * @param <T> A referent to the log on the external system.
 */
public interface ArchivedLog<T> extends AutoCloseable {

  /**
   * Creates an asynchronous reader over this archived log.
   */
  AsyncLogReader createAsyncReader();

  /**
   * Creates a synchronous reader over this archived log.
   * @return
   */
  LogReader createReader();
}
