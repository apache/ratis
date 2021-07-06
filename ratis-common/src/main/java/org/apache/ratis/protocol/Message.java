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
package org.apache.ratis.protocol;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.StringUtils;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * The information clients append to the raft ring.
 */
@FunctionalInterface
public interface Message {
  static Message valueOf(ByteString bytes, Supplier<String> stringSupplier) {
    return new Message() {
      private final MemoizedSupplier<String> memoized = MemoizedSupplier.valueOf(stringSupplier);

      @Override
      public ByteString getContent() {
        return bytes;
      }

      @Override
      public String toString() {
        return memoized.get();
      }
    };
  }

  static Message valueOf(ByteString bytes) {
    return valueOf(bytes, () -> "Message:" + StringUtils.bytes2HexShortString(bytes));
  }

  static Message valueOf(String string) {
    return valueOf(ByteString.copyFromUtf8(string), () -> "Message:" + string);
  }

  static int getSize(Message message) {
    return Optional.ofNullable(message).map(Message::size).orElse(0);
  }

  Message EMPTY = valueOf(ByteString.EMPTY);

  /**
   * @return the content of the message
   */
  ByteString getContent();

  default int size() {
    return Optional.ofNullable(getContent()).map(ByteString::size).orElse(0);
  }
}
