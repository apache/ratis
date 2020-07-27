package org.apache.ratis.experiments.nettyzerocopy.objects;

import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;

public class RequestDataComposite {
  private int dataId;
  private ByteBuf buff;

  public ByteBuf getBuff() {
    return buff;
  }

  public int getDataId() {
    return dataId;
  }

  public void setDataId(int dataId) {
    this.dataId = dataId;
  }

  public void setBuff(ByteBuf buff) {
    this.buff = buff;
  }
}
