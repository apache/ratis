package org.apache.ratis.experiments.nettyzerocopy;

import java.nio.ByteBuffer;

public class RequestData {
  private int dataId;
  //private ByteBuffer buff;

//  public ByteBuffer getBuff() {
//    return buff;
//  }

  public int getDataId() {
    return dataId;
  }

  public void setDataId(int dataId) {
    this.dataId = dataId;
  }

//  public void setBuff(ByteBuffer buff) {
//    this.buff = buff;
//  }
}
