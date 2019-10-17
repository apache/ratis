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
package org.apache.ratis.logservice.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ratis.logservice.api.ArchiveLogWriter;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.util.LogServiceUtils;

public class ArchiveHdfsLogWriter implements ArchiveLogWriter {
  private final Configuration configuration;
  private FileSystem hdfs;
  private FSDataOutputStream os;
  private Path currentPath;
  private long currentRecordId;
  private long lastRollRecordId;

  public ArchiveHdfsLogWriter(Configuration conf) {
    this.configuration = conf;
  }

  public ArchiveHdfsLogWriter() {
    this.configuration = new Configuration();
  }

  @Override public void init(String archiveLocation, LogName logName) throws IOException {
    hdfs = FileSystem.get(configuration);
    Path loc = new Path(LogServiceUtils.getArchiveLocationForLog(archiveLocation, logName));
    if (!hdfs.exists(loc)) {
      hdfs.mkdirs(loc);
    }
    currentPath = new Path(loc, logName.getName());
    os = hdfs.create(currentPath, true);
  }

  @Override public long write(ByteBuffer buffer) throws IOException {
    if (buffer.hasArray()) {
      int startIndex = buffer.arrayOffset();
      int curIndex = buffer.arrayOffset() + buffer.position();
      int endIndex = curIndex + buffer.remaining();
      int length = endIndex - startIndex;
      os.writeInt(length);
      os.write(buffer.array(), startIndex, length);
    } else {
      throw new IllegalArgumentException(
          "Currently array backed byte buffer is only supported for archive write !!");
    }
    currentRecordId++;
    return currentRecordId;
  }

  @Override public List<Long> write(List<ByteBuffer> records) throws IOException {
    List<Long> list = new ArrayList<Long>();
    for (ByteBuffer record : records) {
      list.add(write(record));
    }
    return list;
  }

  @Override public long sync() throws IOException {
    return 0;
  }

  @Override public void close() throws IOException {
    os.close();
    if (lastRollRecordId != currentRecordId) {
      hdfs.rename(currentPath, new Path(currentPath + "_recordId_" + currentRecordId));
    }
  }

  @Override public void rollWriter() throws IOException {
    if (lastRollRecordId != currentRecordId) {
      //close old file
      os.close();
      hdfs.rename(currentPath,
          new Path(LogServiceUtils.getRolledPathForArchiveWriter(currentPath, currentRecordId)));
      lastRollRecordId = currentRecordId;
      //create new file
      os = hdfs.create(currentPath, true);
    }
  }

  @Override public long getLastWrittenRecordId() throws IOException {
    os.hflush();
    return currentRecordId;
  }

}
