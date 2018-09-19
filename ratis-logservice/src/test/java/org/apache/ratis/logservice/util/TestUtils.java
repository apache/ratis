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

import static org.apache.ratis.logservice.util.Utils.bytes2int;
import static org.apache.ratis.logservice.util.Utils.int2bytes;
import static org.apache.ratis.logservice.util.Utils.bytes2long;
import static org.apache.ratis.logservice.util.Utils.long2bytes;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestUtils {

  @Test
  public void testInt2Bytes() {

    byte[] buf = new byte[4];
    int v1 = 0;
    int2bytes(v1, buf, 0);
    int vv1 = bytes2int(buf, 0);
    assertEquals(v1, vv1);
    int v2 = 1;
    int2bytes(v2, buf, 0);
    int vv2 = bytes2int(buf, 0);
    assertEquals(v2, vv2);
    int v3 = -1;
    int2bytes(v3, buf, 0);
    int vv3 = bytes2int(buf, 0);
    assertEquals(v3, vv3);
    int v4 = Integer.MIN_VALUE;
    int2bytes(v4, buf, 0);
    int vv4 = bytes2int(buf, 0);
    assertEquals(v4, vv4);
    int v5 = Integer.MAX_VALUE;
    int2bytes(v5, buf, 0);
    int vv5 = bytes2int(buf, 0);
    assertEquals(v5, vv5);

  }


  @Test
  public void testLong2Bytes() {
    byte[] buf = new byte[8];
    long v1 = 0;
    long2bytes(v1, buf, 0);
    long vv1 = bytes2long(buf, 0);
    assertEquals(v1, vv1);

    long v2 = 1;
    long2bytes(v2, buf, 0);
    long vv2 = bytes2long(buf, 0);
    assertEquals(v2, vv2);

    long v3 = -1;
    long2bytes(v3, buf, 0);
    long vv3 = bytes2long(buf, 0);
    assertEquals(v3, vv3);

    long v4 = Long.MIN_VALUE;
    long2bytes(v4, buf, 0);
    long vv4 = bytes2long(buf, 0);
    assertEquals(v4, vv4);

    long v5 = Integer.MAX_VALUE;
    long2bytes(v5, buf, 0);
    long vv5 = bytes2long(buf, 0);
    assertEquals(v5, vv5);

    long v6 = 100;
    buf = new byte[20];
    long2bytes(v6, buf, 12);
    long vv6 = bytes2long(buf, 12);
    assertEquals(v6, vv6);

  }


}
