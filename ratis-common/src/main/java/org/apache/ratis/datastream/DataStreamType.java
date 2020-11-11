/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.ratis.datastream;

import org.apache.ratis.conf.Parameters;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.ReflectionUtils;

/** The type of data stream implementations. */
public interface DataStreamType {
  /**
   * Parse the given string as a {@link SupportedDataStreamType}
   * or a user-defined {@link DataStreamType}.
   *
   * @param dataStreamType The string representation of an {@link DataStreamType}.
   * @return a {@link SupportedDataStreamType} or a user-defined {@link DataStreamType}.
   */
  static DataStreamType valueOf(String dataStreamType) {
    final Throwable fromSupportedRpcType;
    try { // Try parsing it as a SupportedRpcType
      return SupportedDataStreamType.valueOfIgnoreCase(dataStreamType);
    } catch (Throwable t) {
      fromSupportedRpcType = t;
    }

    try {
      // Try using it as a class name
      return ReflectionUtils.newInstance(ReflectionUtils.getClass(dataStreamType, DataStreamType.class));
    } catch(Throwable t) {
      final String classname = JavaUtils.getClassSimpleName(DataStreamType.class);
      final IllegalArgumentException iae = new IllegalArgumentException(
          "Invalid " + classname + ": \"" + dataStreamType + "\" "
              + " cannot be used as a user-defined " + classname
              + " and it is not a " + JavaUtils.getClassSimpleName(SupportedDataStreamType.class) + ".");
      iae.addSuppressed(t);
      iae.addSuppressed(fromSupportedRpcType);
      throw iae;
    }
  }

  /** @return the name of the rpc type. */
  String name();

  /** @return a new client factory created using the given parameters. */
  DataStreamFactory newClientFactory(Parameters parameters);

  /** @return a new server factory created using the given parameters. */
  DataStreamFactory newServerFactory(Parameters parameters);

  /** An interface to get {@link DataStreamType}. */
  interface Get {
    /** @return the {@link DataStreamType}. */
    DataStreamType getDataStreamType();
  }
}