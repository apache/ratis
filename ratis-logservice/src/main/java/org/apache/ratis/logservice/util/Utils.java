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

public class Utils {


  public static int long2bytes(long v, byte[] buf, int offset) {
    int2bytes((int)(v >>> 32), buf, offset);
    int2bytes((int) v        , buf, offset + 4);
    return 8;
  }

  public static int bytes2int(byte[] buf, int offset) {
    return (buf[offset] << 24)
        + ((0xFF & buf[offset + 1]) << 16)
        + ((0xFF & buf[offset + 2]) <<  8)
        +  (0xFF & buf[offset + 3]);
  }

  public static long bytes2long(byte[] buf, int offset) {
    return ((long)bytes2int(buf, offset) << 32)
        + (0xFFFFFFFFL & bytes2int(buf, offset + 4));
  }

  public static int int2bytes(int v, byte[] buf, int offset) {
    buf[offset    ] = (byte) (v >>> 24);
    buf[offset + 1] = (byte) (v >>> 16);
    buf[offset + 2] = (byte) (v >>> 8);
    buf[offset + 3] = (byte) (v);
    return 4;
  }
}
