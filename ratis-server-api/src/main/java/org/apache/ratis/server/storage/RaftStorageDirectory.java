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
package org.apache.ratis.server.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/** The directory of a {@link RaftStorage}. */
public interface RaftStorageDirectory {
  Logger LOG = LoggerFactory.getLogger(RaftStorageDirectory.class);

  String CURRENT_DIR_NAME = "current";
  String STATE_MACHINE_DIR_NAME = "sm"; // directory containing state machine snapshots
  String TMP_DIR_NAME = "tmp";

  /** @return the root directory of this storage */
  File getRoot();

  /** @return the current directory. */
  default File getCurrentDir() {
    return new File(getRoot(), CURRENT_DIR_NAME);
  }

  /** @return the state machine directory. */
  default File getStateMachineDir() {
    return new File(getRoot(), STATE_MACHINE_DIR_NAME);
  }

  /** @return the temporary directory. */
  default File getTmpDir() {
    return new File(getRoot(), TMP_DIR_NAME);
  }

  /** Is this storage healthy? */
  boolean isHealthy();
}
