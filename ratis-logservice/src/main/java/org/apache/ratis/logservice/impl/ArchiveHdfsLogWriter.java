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
  private FileSystem hdfs;
  private FSDataOutputStream os;
  private Path currentPath;
  private long currentRecordId;
  private long lastRollRecordId;

  @Override
  public void init(String location, LogName logName) throws IOException {
    Configuration configuration = new Configuration();
    hdfs = FileSystem.get(configuration);
    Path loc = new Path(location);
    if(!hdfs.exists(loc)){
      hdfs.mkdirs(loc);
    }
    currentPath = new Path(loc, logName.getName());
    os = hdfs.create(currentPath,true);
  }

  @Override public long write(ByteBuffer data) throws IOException {
    os.writeInt(data.array().length);
    os.write(data.array(), data.arrayOffset(), data.remaining());
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
      hdfs.rename(currentPath, new Path(currentPath + ".recordId_" + currentRecordId));
    }
    hdfs.close();
  }

  @Override public void rollWriter() throws IOException {
    if (lastRollRecordId != currentRecordId) {
      //close old file
      os.close();
      hdfs.rename(currentPath, new Path(currentPath + ".recordId_" + currentRecordId));
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
