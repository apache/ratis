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
package org.apache.ratis.logservice.api;

import java.nio.charset.Charset;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.shaded.proto.logservice.LogServiceProtos;

import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.com.google.protobuf.InvalidProtocolBufferException;

public class LogMessage implements Message {
  public static final Charset UTF8 = Charset.forName("UTF-8");
  /*
   * Type of message
   */
  public static enum Type {
    READ_REQUEST, READ_REPLY, WRITE
  }

  /*
   * For all READ and WRITE requests
   */
  private final LogName name;

  /*
   * Set only for READ_REPLY response
   */
  private long  length;

  /*
   * Pay load for WRITE request
   */
  private byte[] data;

  /*
   * Type of message
   */

  private Type type;

  /**
   * Constructor for WRITE request
   * @param logName name of a log
   * @param data  pay load data
   */
  public LogMessage(LogName logName, byte[] data) {
    this.name = logName;
    this.data = data;
    this.type = Type.WRITE;
  }

  /**
   * Constructor for READ reply
   * @param logName name of a log
   * @param length length of a log
   */
  public LogMessage(LogName logName, long length) {
    this.name = logName;
    this.length = length;
    this.type = Type.READ_REPLY;
  }

  /**
   * Constructor for READ request
   * @param logName name of a log
   */
  public LogMessage(LogName logName) {
    this.name = logName;
    this.type = Type.READ_REQUEST;
  }

  public static LogMessage parseFrom(ByteString data)
      throws InvalidProtocolBufferException {
    LogServiceProtos.LogMessage msg = LogServiceProtos.LogMessage.parseFrom(data);
    LogServiceProtos.MessageType type = msg.getType();
    long length = 0;
    LogName name = null;
    byte[] bdata = null;
    name = LogName.of(msg.getLogName());
    switch (type) {
      case READ_REPLY:
        length = msg.getLength();
        return new LogMessage(name, length);
      case READ_REQUEST:
        return new LogMessage(name);
      case WRITE:
        bdata = msg.getData().toByteArray();
        return new LogMessage(name, bdata);
      default:
        //TODO replace exception
        throw new RuntimeException("Wrong message type: "+ type);
    }
  }


  /**
   * Get log name
   * @return log name
   */
  public LogName getLogName() {
    return name;
  }

  /**
   * Get log length
   * @return log length
   */
  public long getLength() {
    return length;
  }

  /**
   * Get log message data
   * @return data
   */
  public byte[] getData() {
    return data;
  }

  /**
   * Get message type
   * @return message type
   */
  public Type getType() {
    return type;
  }


  @SuppressWarnings("deprecation")
  @Override
  public ByteString getContent() {
    LogServiceProtos.LogMessage.Builder builder = LogServiceProtos.LogMessage.newBuilder();
    builder.setType(LogServiceProtos.MessageType.valueOf(type.ordinal()));
    builder.setLogName(name.getName());
    switch (type) {
      case READ_REPLY:
        builder.setLength(length);
        break;
      case WRITE:
        builder.setData(ByteString.copyFrom(data));
        break;
      default:
    }

    return builder.build().toByteString();
  }

  @Override
  public String toString() {
    String s = type.name() + " : log=" + name.getName();
    if (type == Type.READ_REPLY) {
      s += " len=" + length;
    } else if (type == Type.WRITE) {
      s += " data len=" + data.length;
    }
    return s;
  }
}
