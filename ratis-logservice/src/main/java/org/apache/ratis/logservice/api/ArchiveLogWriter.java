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

import java.io.IOException;

public interface ArchiveLogWriter extends LogWriter{

  /**
   * Initializes the writer
   * @param location archival location
   * @param logName
   * @throws IOException
   */
  void init(String location, LogName logName) throws IOException;

  /**
   * Rolls writer after number of records written crosses threshold
   * {@link org.apache.ratis.logservice.server.LogStateMachine#DEFAULT_ARCHIVE_THRESHOLD_PER_FILE}
   *
   * @throws IOException
   */
  void rollWriter() throws IOException;

  /**
   * Record Id of the last written record
   * @return
   */
  long getLastWrittenRecordId() throws IOException;
}
