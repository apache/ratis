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

import org.apache.ratis.thirdparty.com.google.protobuf.AbstractMessage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.TextFormat;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.StringUtils;

import java.nio.ByteBuffer;
import java.util.Objects;
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
      public int hashCode() {
        return Objects.hashCode(bytes);
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == this) {
          return true;
        } else if (!(obj instanceof Message)) {
          return false;
        } else {
          final Message that = (Message)obj;
          if (that.isByteBufferSupported()) {
            return Objects.equals(this.asReadOnlyByteBuffer(), that.asReadOnlyByteBuffer());
          } else {
            return Objects.equals(this.getContent(), that.getContent());
          }
        }
      }

      @Override
      public String toString() {
        return memoized.get();
      }
    };
  }

  static Message valueOf(AbstractMessage abstractMessage) {
    return valueOf(abstractMessage.toByteString(), () -> TextFormat.shortDebugString(abstractMessage));
  }

  static Message valueOf(ByteString bytes) {
    return valueOf(bytes, () -> "Message:" + StringUtils.bytes2ShortString(bytes));
  }

  static Message valueOf(ByteBuffer bytes) {
    return valueOf(ByteString.copyFrom(bytes));
  }

  static Message valueOf(String string) {
    return valueOf(ByteString.copyFromUtf8(string), () -> "Message:" + string);
  }

  /**
   * Convert the content of the given message to a {@link ByteString}.
   * When the message supports {@link ByteBuffer},
   * the buffer is wrapped without copying and {@link #getContent()} is not invoked.
   *
   * @return the content as a {@link ByteString}, or null if the content is null.
   */
  static ByteString toByteString(Message message) {
    if (message == null) {
      return null;
    }
    if (!message.isByteBufferSupported()) {
      return message.getContent();
    }
    final ByteBuffer buffer = message.asReadOnlyByteBuffer();
    return buffer == null ? null : UnsafeByteOperations.unsafeWrap(buffer);
  }

  Message EMPTY = valueOf(ByteString.EMPTY);

  /**
   * @return the content of the message.
   *         When {@link #isByteBufferSupported()} returns true,
   *         Ratis never invokes this method and the implementation may throw {@link UnsupportedOperationException}.
   */
  ByteString getContent();

  /**
   * When it returns true, Ratis uses {@link #asReadOnlyByteBuffer()} instead of {@link #getContent()}.
   *
   * @return true if this message supports {@link ByteBuffer}; otherwise, return false.
   */
  default boolean isByteBufferSupported() {
    return false;
  }

  /** The same as {@link ByteBuffer#asReadOnlyBuffer()}. */
  default ByteBuffer asReadOnlyByteBuffer() {
    final ByteString content = getContent();
    return content == null ? null : content.asReadOnlyByteBuffer();
  }

  static int getSize(Message message) {
    return message == null ? 0 : message.size();
  }

  static int getSize(ByteBuffer buffer) {
    return buffer == null ? 0 : buffer.remaining();
  }

  static int getSize(ByteString bytes) {
    return bytes == null ? 0 : bytes.size();
  }

  default int size() {
    return isByteBufferSupported() ? getSize(asReadOnlyByteBuffer()) : getSize(getContent());
  }
}
