/*
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

package org.apache.ratis.tools;

import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;
import org.apache.ratis.server.raftlog.segmented.LogSegment;
import org.apache.ratis.util.ReferenceCountedObject;
import org.apache.ratis.util.SizeInBytes;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;

public final class ParseRatisLog {

  private final File file;
  private final Function<StateMachineLogEntryProto, String> smLogToString;
  private final SizeInBytes maxOpSize;

  private long numConfEntries;
  private long numMetadataEntries;
  private long numStateMachineEntries;
  private long numInvalidEntries;

  private ParseRatisLog(File f , Function<StateMachineLogEntryProto, String> smLogToString, SizeInBytes maxOpSize) {
    this.file = f;
    this.smLogToString = smLogToString;
    this.maxOpSize = maxOpSize;
    this.numConfEntries = 0;
    this.numMetadataEntries = 0;
    this.numStateMachineEntries = 0;
    this.numInvalidEntries = 0;
  }

  public void dumpSegmentFile() throws IOException {
    final LogSegmentPath pi = LogSegmentPath.matchLogSegment(file.toPath());
    if (pi == null) {
      System.out.println("Invalid segment file");
      return;
    }

    System.out.println("Processing Raft Log file: " + file.getAbsolutePath() + " size:" + file.length());
    final int entryCount = LogSegment.readSegmentFile(file, pi.getStartEnd(), maxOpSize,
        RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION, null, this::processLogEntry);
    System.out.println("Num Total Entries: " + entryCount);
    System.out.println("Num Conf Entries: " + numConfEntries);
    System.out.println("Num Metadata Entries: " + numMetadataEntries);
    System.out.println("Num StateMachineEntries Entries: " + numStateMachineEntries);
    System.out.println("Num Invalid Entries: " + numInvalidEntries);
  }


  private void processLogEntry(ReferenceCountedObject<LogEntryProto> ref) {
    final LogEntryProto proto = ref.retain();
    if (proto.hasConfigurationEntry()) {
      numConfEntries++;
    } else if (proto.hasMetadataEntry()) {
      numMetadataEntries++;
    } else if (proto.hasStateMachineLogEntry()) {
      numStateMachineEntries++;
    } else {
      System.out.println("Found an invalid entry: " + proto);
      numInvalidEntries++;
    }

    String str = LogProtoUtils.toLogEntryString(proto, smLogToString);
    System.out.println(str);
    ref.release();
  }

  public static class Builder {
    private File file = null;
    private Function<StateMachineLogEntryProto, String> smLogToString = null;
    private SizeInBytes maxOpSize = SizeInBytes.valueOf("32MB");

    public Builder setMaxOpSize(SizeInBytes maxOpSize) {
      this.maxOpSize = maxOpSize;
      return this;
    }

    public Builder setSegmentFile(File segmentFile) {
      this.file = segmentFile;
      return this;
    }

    public Builder setSMLogToString(Function<StateMachineLogEntryProto, String> smLogToStr) {
      this.smLogToString = smLogToStr;
      return this;
    }

    public ParseRatisLog build() {
      return new ParseRatisLog(file, smLogToString, maxOpSize);
    }
  }
}
