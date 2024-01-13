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
package org.apache.ratis.grpc.util;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.com.google.protobuf.MessageLite;
import org.apache.ratis.thirdparty.com.google.protobuf.Parser;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.ratis.thirdparty.io.grpc.Detachable;
import org.apache.ratis.thirdparty.io.grpc.HasByteBuffer;
import org.apache.ratis.thirdparty.io.grpc.KnownLength;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor.PrototypeMarshaller;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.protobuf.lite.ProtoLiteUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Custom gRPC marshaller to use zero memory copy feature of gRPC when deserializing messages. This
 * achieves zero-copy by deserializing proto messages pointing to the buffers in the input stream to
 * avoid memory copy so stream should live as long as the message can be referenced. Hence, it
 * exposes the input stream to applications (through popStream) and applications are responsible to
 * close it when it's no longer needed. Otherwise, it'd cause memory leak.
 */
public class ZeroCopyMessageMarshaller<T extends MessageLite> implements PrototypeMarshaller<T> {
  static final Logger LOG = LoggerFactory.getLogger(ZeroCopyMessageMarshaller.class);

  private final String name;
  private final Map<T, InputStream> unclosedStreams = Collections.synchronizedMap(new IdentityHashMap<>());
  private final Parser<T> parser;
  private final PrototypeMarshaller<T> marshaller;

  private final Consumer<T> zeroCopyCount;
  private final Consumer<T> nonZeroCopyCount;
  private final Consumer<T> releasedCount;

  public ZeroCopyMessageMarshaller(T defaultInstance) {
    this(defaultInstance, m -> {}, m -> {}, m -> {});
  }

  public ZeroCopyMessageMarshaller(T defaultInstance, Consumer<T> zeroCopyCount, Consumer<T> nonZeroCopyCount,
      Consumer<T> releasedCount) {
    this.name = JavaUtils.getClassSimpleName(defaultInstance.getClass()) + "-Marshaller";
    @SuppressWarnings("unchecked")
    final Parser<T> p = (Parser<T>) defaultInstance.getParserForType();
    this.parser = p;
    this.marshaller = (PrototypeMarshaller<T>) ProtoLiteUtils.marshaller(defaultInstance);

    this.zeroCopyCount = zeroCopyCount;
    this.nonZeroCopyCount = nonZeroCopyCount;
    this.releasedCount = releasedCount;
  }

  @Override
  public Class<T> getMessageClass() {
    return marshaller.getMessageClass();
  }

  @Override
  public T getMessagePrototype() {
    return marshaller.getMessagePrototype();
  }

  @Override
  public InputStream stream(T value) {
    return marshaller.stream(value);
  }

  @Override
  public T parse(InputStream stream) {
    final T message;
    try {
      // fast path (no memory copy)
      message = parseZeroCopy(stream);
    } catch (IOException e) {
      throw Status.INTERNAL
          .withDescription("Failed to parseZeroCopy")
          .withCause(e)
          .asRuntimeException();
    }
    if (message != null) {
      zeroCopyCount.accept(message);
      return message;
    }

    // slow path
    final T copied = marshaller.parse(stream);
    nonZeroCopyCount.accept(copied);
    return copied;
  }

  /** Release the underlying buffers in the given message. */
  public void release(T message) {
    final InputStream stream = popStream(message);
    if (stream == null) {
      return;
    }
    try {
      stream.close();
      releasedCount.accept(message);
    } catch (IOException e) {
      LOG.error(name + ": Failed to close stream.", e);
    }
  }

  private List<ByteString> getByteStrings(InputStream detached, int exactSize) throws IOException {
    Preconditions.assertTrue(detached instanceof HasByteBuffer);

    // This mark call is to keep buffer while traversing buffers using skip.
    detached.mark(exactSize);
    final List<ByteString> byteStrings = new LinkedList<>();
    while (detached.available() != 0) {
      final ByteBuffer buffer = ((HasByteBuffer)detached).getByteBuffer();
      Objects.requireNonNull(buffer, "buffer == null");
      byteStrings.add(UnsafeByteOperations.unsafeWrap(buffer));
      final int remaining = buffer.remaining();
      final long skipped = detached.skip(buffer.remaining());
      Preconditions.assertSame(remaining, skipped, "skipped");
    }
    detached.reset();
    return byteStrings;
  }

  /**
   * Use a zero copy method to parse a message from the given stream.
   *
   * @return the parsed message if the given stream support zero copy; otherwise, return null.
   */
  private T parseZeroCopy(InputStream stream) throws IOException {
    if (!(stream instanceof KnownLength)) {
      LOG.debug("stream is not KnownLength: {}", stream.getClass());
      return null;
    }
    if (!(stream instanceof Detachable)) {
      LOG.debug("stream is not Detachable: {}", stream.getClass());
      return null;
    }
    if (!(stream instanceof HasByteBuffer)) {
      LOG.debug("stream is not HasByteBuffer: {}", stream.getClass());
      return null;
    }
    if (!((HasByteBuffer) stream).byteBufferSupported()) {
      LOG.debug("stream is HasByteBuffer but not byteBufferSupported: {}", stream.getClass());
      return null;
    }

    final int exactSize = stream.available();
    InputStream detached = ((Detachable) stream).detach();
    try {
      final List<ByteString> byteStrings = getByteStrings(detached, exactSize);
      final T message = parseFrom(byteStrings, exactSize);

      final InputStream previous = unclosedStreams.put(message, detached);
      Preconditions.assertNull(previous, "previous");

      detached = null;
      return message;
    } finally {
      if (detached != null) {
        detached.close();
      }
    }
  }

  private T parseFrom(List<ByteString> byteStrings, int exactSize) {
    final CodedInputStream codedInputStream = ByteString.copyFrom(byteStrings).newCodedInput();
    codedInputStream.enableAliasing(true);
    codedInputStream.setSizeLimit(exactSize);

    try {
      return parseFrom(codedInputStream);
    } catch (InvalidProtocolBufferException e) {
      throw Status.INTERNAL
          .withDescription("Invalid protobuf byte sequence")
          .withCause(e)
          .asRuntimeException();
    }
  }

  private T parseFrom(CodedInputStream stream) throws InvalidProtocolBufferException {
    final T message = parser.parseFrom(stream);
    try {
      stream.checkLastTagWas(0);
      return message;
    } catch (InvalidProtocolBufferException e) {
      e.setUnfinishedMessage(message);
      throw e;
    }
  }

  /**
   * Application can call this function to get the stream for the message, and,
   * possibly later, must call {@link InputStream#close()} to release buffers.
   * Alternatively, use {@link #release(T)} to do both in one step.
   */
  public InputStream popStream(T message) {
    return unclosedStreams.remove(message);
  }
}
