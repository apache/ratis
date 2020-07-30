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

package org.apache.ratis.experiments.nettyzerocopy.objects;

import java.nio.ByteBuffer;

public class RequestData {
  private int dataId;
  private ByteBuffer buff;

  public ByteBuffer getBuff() {
    return buff;
  }

  public int getDataId() {
    return dataId;
  }

  public void setDataId(int dataId) {
    this.dataId = dataId;
  }

  public void setBuff(ByteBuffer buff) {
    this.buff = buff;
  }

  public boolean verifyData(){
    System.out.println(buff.capacity());
    boolean status = true;
    for(int i = 0; i < buff.capacity(); i++){
      if(buff.get() != (byte)'a'){
        status = false;
      }
    }
    buff.flip();
    return status;
  }
}
