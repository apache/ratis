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
