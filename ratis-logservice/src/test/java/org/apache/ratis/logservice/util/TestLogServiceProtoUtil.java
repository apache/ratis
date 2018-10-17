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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.proto.logservice.LogServiceProtos.AppendLogEntryReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.AppendLogEntryRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.ArchiveLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.ArchiveLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.CloseLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.CloseLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.CreateLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.CreateLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.DeleteLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.DeleteLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetLogLengthReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetLogLengthRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetLogStartIndexReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetLogStartIndexRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetStateReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.GetStateRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.ListLogsRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.LogServiceRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.ReadLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.ReadLogRequestProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.SyncLogReplyProto;
import org.apache.ratis.proto.logservice.LogServiceProtos.SyncLogRequestProto;
import org.junit.Ignore;
import org.junit.Test;

public class TestLogServiceProtoUtil {


  @Test
  public void testAppendRequest() {
    LogName name = LogName.of("test");
    List<byte[]> entries = new ArrayList<byte[]>();
    byte[] e1 = new byte[] {1,1};
    byte[] e2 = new byte[] {2,2};
    entries.add(e1);
    entries.add(e2);

    LogServiceRequestProto proto = LogServiceProtoUtil.toAppendEntryLogRequestProto(name, entries);
    AppendLogEntryRequestProto request = proto.getAppendRequest();
    assertEquals(name.getName(), request.getLogName().getName());
    assertEquals(2, request.getDataCount());
    assertTrue(TestUtils.equals(e1, request.getData(0).toByteArray()));
    assertTrue(TestUtils.equals(e2, request.getData(1).toByteArray()));

  }

  @Test
  public void testAppendReply() {
    List<byte[]> entries = new ArrayList<byte[]>();
    byte[] e1 = new byte[] {1,1};
    byte[] e2 = new byte[] {2,2};
    entries.add(e1);
    entries.add(e2);

    AppendLogEntryReplyProto proto =
        LogServiceProtoUtil.toAppendLogReplyProto(null, null);
    //TODO finish test
  }

  @Test
  public void testReadRequest() {
    LogName name = LogName.of("test");

    long start = 100;
    int total = 5;
    LogServiceRequestProto proto = LogServiceProtoUtil.toReadLogRequestProto(name, start, total);
    ReadLogRequestProto request = proto.getReadNextQuery();
    assertEquals(name.getName(), request.getLogName().getName());
    assertEquals(100, request.getStartRecordId());
    assertEquals(5, request.getNumRecords());

  }

  @Test
  public void testReadReply() {
    List<byte[]> entries = new ArrayList<byte[]>();
    byte[] e1 = new byte[] {1,1};
    byte[] e2 = new byte[] {2,2};
    entries.add(e1);
    entries.add(e2);

    ReadLogReplyProto proto =
        LogServiceProtoUtil.toReadLogReplyProto(entries, null);

    assertEquals(2, proto.getLogRecordCount());
    assertTrue(TestUtils.equals(e1, proto.getLogRecord(0).toByteArray()));
    assertTrue(TestUtils.equals(e2, proto.getLogRecord(1).toByteArray()));
  }

  @Test
  public void testGetLengthReply() {

    long len = 100;
    GetLogLengthReplyProto proto =
        LogServiceProtoUtil.toGetLogLengthReplyProto(len, null);
    assertEquals(len, proto.getLength());
  }

  @Test
  public void testGetLengthRequest() {
    LogName name = LogName.of("test");
    LogServiceRequestProto proto = LogServiceProtoUtil.toGetLengthRequestProto(name);
    GetLogLengthRequestProto request = proto.getLengthQuery();
    assertEquals(name.getName(), request.getLogName().getName());
  }

  @Test
  public void testGetStartIndexRequest() {
    LogName name = LogName.of("test");
    LogServiceRequestProto proto = LogServiceProtoUtil.toGetStartIndexProto(name);
    GetLogStartIndexRequestProto request = proto.getStartIndexQuery();
    assertEquals(name.getName(), request.getLogName().getName());
  }

  @Test
  public void testGetStartIndexReply() {

    long index = 100;
    GetLogStartIndexReplyProto proto =
        LogServiceProtoUtil.toGetLogStartIndexReplyProto(index, null);
    assertEquals(index, proto.getStartIndex());
  }

  @Test
  public void testSyncRequest() {
    LogName name = LogName.of("test");
    LogServiceRequestProto proto = LogServiceProtoUtil.toSyncLogRequestProto(name);
    SyncLogRequestProto request = proto.getSyncRequest();
    assertEquals(name.getName(), request.getLogName().getName());
  }

  @Test
  public void testSyncReply() {

    SyncLogReplyProto proto =
        LogServiceProtoUtil.toSyncLogReplyProto(null);
    //TODO finish test
  }


  //LIST LOGS
  @Test
  public void testListLogsRequest() {
    LogServiceRequestProto proto = LogServiceProtoUtil.toListLogRequestProto();
    ListLogsRequestProto request = proto.getListLogs();
    //TODO finish
  }

  @Test
  public void testListLogsReply() {

    //TODO finish test
  }

  //GET LOG

  @Test
  public void testGetLogRequest() {
    LogName name = LogName.of("test");
    LogServiceRequestProto proto = LogServiceProtoUtil.toGetLogRequestProto(name);
    GetLogRequestProto request = proto.getGetLog();
    assertEquals(name.getName(), request.getLogName().getName());
  }

  @Test
  @Ignore
  public void testGetLogReply() {
    LogStream logStream = null;
    GetLogReplyProto proto = LogServiceProtoUtil.toGetLogReplyProto(logStream);
    //TODO finish

  }

  //GET STATE
  @Test
  public void testGetStateRequest() {
    LogName name = LogName.of("test");
    LogServiceRequestProto proto = LogServiceProtoUtil.toGetStateRequestProto(name);
    GetStateRequestProto request = proto.getGetState();
    assertEquals(name.getName(), request.getLogName().getName());
    //TODO finish
  }

  @Test
  @Ignore
  public void testGetStateReply() {
    LogStream logStream = null;
    GetStateReplyProto proto = LogServiceProtoUtil.toGetStateReplyProto(true);
    //TODO finish

  }
  //CREATE LOG
  @Test
  public void testCreateLogRequest() {
    LogName name = LogName.of("test");
    LogServiceRequestProto proto = LogServiceProtoUtil.toCreateLogRequestProto(name);
    CreateLogRequestProto request = proto.getCreateLog();
    assertEquals(name.getName(), request.getLogName().getName());
    //TODO finish
  }

  @Test
  @Ignore
  public void testCreateLogReply() {
    LogStream logStream = null;
    CreateLogReplyProto proto = LogServiceProtoUtil.toCreateLogReplyProto(logStream);
    //TODO finish

  }
  //ARCHIVE LOG
  @Test
  public void testArchiveLogRequest() {
    LogName name = LogName.of("test");
    LogServiceRequestProto proto = LogServiceProtoUtil.toArchiveLogRequestProto(name);
    ArchiveLogRequestProto request = proto.getArchiveLog();
    assertEquals(name.getName(), request.getLogName().getName());
    //TODO finish
  }

  @Test
  @Ignore
  public void testArchiveLogReply() {
    ArchiveLogReplyProto proto = LogServiceProtoUtil.toArchiveLogReplyProto();
    //TODO finish

  }
  //DELETE LOG
  @Test
  public void testDeleteLogRequest() {
    LogName name = LogName.of("test");
    LogServiceRequestProto proto = LogServiceProtoUtil.toDeleteLogRequestProto(name);
    DeleteLogRequestProto request = proto.getDeleteLog();
    assertEquals(name.getName(), request.getLogName().getName());
    //TODO finish
  }

  @Test
  @Ignore
  public void testDeleteLogReply() {
    DeleteLogReplyProto proto = LogServiceProtoUtil.toDeleteLogReplyProto();
    //TODO finish

  }
  //CLOSE LOG
  @Test
  public void testCloseLogRequest() {
    LogName name = LogName.of("test");
    LogServiceRequestProto proto = LogServiceProtoUtil.toCloseLogRequestProto(name);
    CloseLogRequestProto request = proto.getCloseLog();
    assertEquals(name.getName(), request.getLogName().getName());
    //TODO finish
  }

  @Test
  @Ignore
  public void testCloseLogReply() {
    CloseLogReplyProto proto = LogServiceProtoUtil.toCloseLogReplyProto();
    //TODO finish

  }
}
