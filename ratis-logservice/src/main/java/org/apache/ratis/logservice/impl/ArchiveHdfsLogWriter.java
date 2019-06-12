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
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.ratis.logservice.api.ArchiveLogWriter;

public class ArchiveHdfsLogWriter implements ArchiveLogWriter {
  private FileSystem hdfs;
  private FSDataOutputStream os;
  private Path currentPath;
  private long currentRecordId;
  private long lastRollRecordId;

  @Override
  public void init(String file) throws IOException {
    Configuration configuration = new Configuration();
    hdfs = FileSystem.get(configuration);
    currentPath = new Path(file);
    os = hdfs.create(currentPath,true);
  }

  @Override public long write(ByteBuffer data) throws IOException {
    os.writeInt(data.array().length);
    os.write(data.array());
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
    renameIfNewData();
    hdfs.close();
  }

  @Override public void rollWriter() throws IOException {
    //close old file
    os.close();
    renameIfNewData();
    lastRollRecordId = currentRecordId;
    //create new file
    os = hdfs.create(currentPath, true);
  }

  @Override public long getLastWrittenRecordId() {
    return currentRecordId;
  }

  private void renameIfNewData() throws IOException {
    if (lastRollRecordId != currentRecordId) {
      hdfs.rename(currentPath, new Path(currentPath + ".recordId_" + currentRecordId));
    }
  }
}
