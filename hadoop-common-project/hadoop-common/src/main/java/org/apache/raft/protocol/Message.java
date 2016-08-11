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
package org.apache.raft.protocol;

/**
 * The information clients append to the raft ring.
 */
public interface Message {
  byte[] EMPTY_BYTE_ARRAY = {};

  /** Empty message */
  Message EMPTY_MESSAGE = new Message() {
    @Override
    public String toString() {
      return "";
    }

    @Override
    public byte[] getContent() {
      return EMPTY_BYTE_ARRAY;
    }
  };

  /**
   * @return the content of the message
   */
  byte[] getContent();
}
