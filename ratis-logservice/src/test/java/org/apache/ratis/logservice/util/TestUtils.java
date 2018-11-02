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
package org.apache.ratis.logservice.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestUtils {

  public static boolean equals(byte[] a, byte[] b) {
    if (a == null || b == null) {
      return false;
    }
    if (a.length != b.length) {
      return false;
    }
    for(int i=0; i < a.length; i++) {
      if (a[i] != b[i]) {
        return false;
      }
    }
    return true;
  }

  public static List<ByteBuffer> getRandomData(int dataSize, int totalRecords) {
    byte[][] data = new byte[totalRecords][dataSize];
    Random r = new Random();
    for(int i=0; i < data.length; i++) {
      data[i] = new byte[dataSize];
      r.nextBytes(data[i]);
    }
    List<ByteBuffer> list = new ArrayList<ByteBuffer>();
    for (int i=0; i < data.length; i++) {
      list.add(ByteBuffer.wrap(data[i]));
    }
    return list;
  }
}
