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

package org.apache.ratis.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper to load the native ratis code i.e. libratis.so.
 * This handles the fallback to either the bundled libratis-Linux-i386-32.so
 * or the default java implementations where appropriate.
 */
public final class NativeCodeLoader {

  private static final Logger LOG = LoggerFactory.getLogger(NativeCodeLoader.class);

  private static boolean nativeCodeLoaded = false;

  static {
    // Try to load native ratis library and set fallback flag appropriately
    LOG.debug("Trying to load the custom-built native-ratis library...");
    try {
      System.loadLibrary("ratis");
      LOG.debug("Loaded the native-ratis library");
      nativeCodeLoaded = true;
    } catch (Throwable t) {
      // Ignore failure to load
      if(LOG.isDebugEnabled()) {
        LOG.debug("Failed to load native-ratis with error: " + t);
        LOG.debug("java.library.path=" +
            System.getProperty("java.library.path"));
      }
    }

    if (!nativeCodeLoaded) {
      LOG.warn("Unable to load native-ratis library for your platform... " +
               "using builtin-java classes where applicable");
    }
  }

  private NativeCodeLoader() {}

  /**
   * Check if native-ratis code is loaded for this platform.
   *
   * @return <code>true</code> if native-ratis is loaded,
   *         else <code>false</code>
   */
  public static boolean isNativeCodeLoaded() {
    return nativeCodeLoaded;
  }

  public static native String getLibraryName();
}
