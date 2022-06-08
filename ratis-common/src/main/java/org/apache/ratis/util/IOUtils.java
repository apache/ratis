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

import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.ObjectInputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * IO related utility methods.
 */
public interface IOUtils {
  static InterruptedIOException toInterruptedIOException(
      String message, InterruptedException e) {
    final InterruptedIOException iioe = new InterruptedIOException(message);
    iioe.initCause(e);
    return iioe;
  }

  static IOException asIOException(Throwable t) {
    Objects.requireNonNull(t, "t == null");
    return t instanceof IOException? (IOException)t : new IOException(t);
  }

  static IOException toIOException(ExecutionException e) {
    final Throwable cause = e.getCause();
    return cause != null? asIOException(cause): new IOException(e);
  }

  static <T> T getFromFuture(CompletableFuture<T> future, Supplier<Object> name) throws IOException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw toInterruptedIOException(name.get() + " interrupted.", e);
    } catch (ExecutionException e) {
      throw toIOException(e);
    } catch (CompletionException e) {
      throw asIOException(JavaUtils.unwrapCompletionException(e));
    }
  }

  static <T> T getFromFuture(CompletableFuture<T> future, Supplier<Object> name, TimeDuration timeout)
      throws IOException {
    try {
      return future.get(timeout.getDuration(), timeout.getUnit());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw toInterruptedIOException(name.get() + " interrupted.", e);
    } catch (ExecutionException e) {
      throw toIOException(e);
    } catch (CompletionException e) {
      throw asIOException(JavaUtils.unwrapCompletionException(e));
    } catch(TimeoutException e) {
      throw new TimeoutIOException("Timeout " + timeout + ": " + name.get(), e);
    }
  }

  static boolean shouldReconnect(Throwable e) {
    return ReflectionUtils.isInstance(e,
        SocketException.class, SocketTimeoutException.class, ClosedChannelException.class, EOFException.class,
        AlreadyClosedException.class);
  }

  static void readFully(InputStream in, int buffSize) throws IOException {
    final byte [] buf = new byte[buffSize];
    for(int bytesRead = in.read(buf); bytesRead >= 0; ) {
      bytesRead = in.read(buf);
    }
  }

  /**
   * Reads len bytes in a loop.
   *
   * @param in InputStream to read from
   * @param buf The buffer to fill
   * @param off offset from the buffer
   * @param len the length of bytes to read
   * @throws IOException if it could not read requested number of bytes
   * for any reason (including EOF)
   */
  static void readFully(InputStream in, byte[] buf, int off, int len)
      throws IOException {
    for(int toRead = len; toRead > 0; ) {
      final int ret = in.read(buf, off, toRead);
      if (ret < 0) {
        final int read = len - toRead;
        throw new EOFException("Premature EOF: read length is " + len + " but encountered EOF at " + read);
      }
      toRead -= ret;
      off += ret;
    }
  }

  /**
   * Write a ByteBuffer to a FileChannel at a given offset,
   * handling short writes.
   *
   * @param fc               The FileChannel to write to
   * @param buf              The input buffer
   * @param offset           The offset in the file to start writing at
   * @throws IOException     On I/O error
   */
  static void writeFully(FileChannel fc, ByteBuffer buf, long offset)
      throws IOException {
    do {
      offset += fc.write(buf, offset);
    } while (buf.remaining() > 0);
  }

  static long preallocate(FileChannel fc, long size, ByteBuffer fill) throws IOException {
    Preconditions.assertSame(0, fill.position(), "fill.position");
    Preconditions.assertSame(fill.capacity(), fill.limit(), "fill.limit");
    final int remaining = fill.remaining();

    long allocated = 0;
    for(; allocated < size; ) {
      final long required = size - allocated;
      final int n = remaining < required? remaining: Math.toIntExact(required);
      final ByteBuffer buffer = fill.slice();
      buffer.limit(n);
      IOUtils.writeFully(fc, buffer, fc.size());
      allocated += n;
    }
    return allocated;
  }

  /**
   * Similar to readFully(). Skips bytes in a loop.
   * @param in The InputStream to skip bytes from
   * @param len number of bytes to skip.
   * @throws IOException if it could not skip requested number of bytes
   * for any reason (including EOF)
   */
  static void skipFully(InputStream in, long len) throws IOException {
    long amt = len;
    while (amt > 0) {
      long ret = in.skip(amt);
      if (ret == 0) {
        // skip may return 0 even if we're not at EOF.  Luckily, we can
        // use the read() method to figure out if we're at the end.
        int b = in.read();
        if (b == -1) {
          throw new EOFException( "Premature EOF from inputStream after " +
              "skipping " + (len - amt) + " byte(s).");
        }
        ret = 1;
      }
      amt -= ret;
    }
  }

  /**
   * Close the Closeable objects and <b>ignore</b> any {@link Throwable} or
   * null pointers. Must only be used for cleanup in exception handlers.
   *
   * @param log the log to record problems to at debug level. Can be null.
   * @param closeables the objects to close
   */
  static void cleanup(Logger log, Closeable... closeables) {
    for (Closeable c : closeables) {
      if (c != null) {
        try {
          c.close();
        } catch(Exception e) {
          if (log != null && log.isDebugEnabled()) {
            log.debug("Exception in closing " + c, e);
          }
        }
      }
    }
  }

  /** Serialize the given object to a byte array using {@link java.io.ObjectOutputStream#writeObject(Object)}. */
  static byte[] object2Bytes(Object obj) {
    return ProtoUtils.writeObject2ByteString(obj).toByteArray();
  }

  static <T> T bytes2Object(byte[] bytes, Class<T> clazz) {
    return readObject(new ByteArrayInputStream(bytes), clazz);
  }

  /**
   * Read an object from the given input stream.
   *
   * @param in input stream to read from.
   * @param clazz the class of the object.
   * @return the object read from the stream.
   *
   * @param <T> The class type.
   */
  static <T> T readObject(InputStream in, Class<T> clazz) {
    final Object obj;
    try(ObjectInputStream oin = new ObjectInputStream(in)) {
      obj = oin.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new IllegalStateException("Failed to readObject for class " + clazz, e);
    }
    try {
      return clazz.cast(obj);
    } catch (ClassCastException e) {
      throw new IllegalStateException("Failed to cast to " + clazz + ", object="
              + (obj instanceof Throwable? StringUtils.stringifyException((Throwable) obj): obj), e);
    }
  }
}
