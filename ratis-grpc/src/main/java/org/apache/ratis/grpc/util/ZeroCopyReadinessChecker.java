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

import org.apache.ratis.thirdparty.com.google.protobuf.MessageLite;
import org.apache.ratis.thirdparty.io.grpc.KnownLength;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checker to test whether a zero-copy masharller is available from the versions of gRPC and
 * Protobuf.
 */
public final class ZeroCopyReadinessChecker {
  static final Logger LOG = LoggerFactory.getLogger(ZeroCopyReadinessChecker.class);
  private static final boolean IS_ZERO_COPY_READY;

  private ZeroCopyReadinessChecker() {
  }

  static {
    // Check whether io.grpc.Detachable exists?
    boolean detachableClassExists = false;
    try {
      // Try to load Detachable interface in the package where KnownLength is in.
      // This can be done directly by looking up io.grpc.Detachable but rather
      // done indirectly to handle the case where gRPC is being shaded in a
      // different package.
      String knownLengthClassName = KnownLength.class.getName();
      String detachableClassName =
          knownLengthClassName.substring(0, knownLengthClassName.lastIndexOf('.') + 1)
              + "Detachable";
      // check if class exists.
      Class.forName(detachableClassName);
      detachableClassExists = true;
    } catch (ClassNotFoundException ex) {
      LOG.debug("io.grpc.Detachable not found", ex);
    }
    // Check whether com.google.protobuf.UnsafeByteOperations exists?
    boolean unsafeByteOperationsClassExists = false;
    try {
      // Same above
      String messageLiteClassName = MessageLite.class.getName();
      String unsafeByteOperationsClassName =
          messageLiteClassName.substring(0, messageLiteClassName.lastIndexOf('.') + 1)
              + "UnsafeByteOperations";
      // check if class exists.
      Class.forName(unsafeByteOperationsClassName);
      unsafeByteOperationsClassExists = true;
    } catch (ClassNotFoundException ex) {
      LOG.debug("com.google.protobuf.UnsafeByteOperations not found", ex);
    }
    IS_ZERO_COPY_READY = detachableClassExists && unsafeByteOperationsClassExists;
  }

  public static boolean isReady() {
    return IS_ZERO_COPY_READY;
  }
}
