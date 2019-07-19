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
package org.apache.ratis.logservice.impl;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import com.google.common.primitives.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.ratis.logservice.api.ArchiveLogReader;
import org.apache.ratis.logservice.api.ArchiveLogWriter;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.util.LogServiceUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestArchiveHdfsLogReaderAndWriter {
  static MiniDFSCluster cluster;
  static Configuration conf;
  private static String location;

  @BeforeClass public static void setup() throws IOException {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    location = "target/tmp/archive/TestArchiveHdfsLogReaderAndWriter";
  }

  @Test public void testRollingWriter() throws IOException {
    String archiveLocation = location+"/testRollingWriter";
    LogName logName = LogName.of("testRollingWriterLogName");
    DistributedFileSystem fs = cluster.getFileSystem();
    fs.delete(new Path(archiveLocation), true);
    ArchiveLogWriter writer = new ArchiveHdfsLogWriter(conf);
    writer.init(archiveLocation, logName);
    int k = 2;
    write(writer, 1, k);
    Assert.assertEquals(writer.getLastWrittenRecordId(), k);
    writer.rollWriter();
    String[] files = Arrays.stream(
        fs.listStatus(new Path(LogServiceUtils.getArchiveLocationForLog(archiveLocation, logName))))
        .map(fileStatus -> fileStatus.getPath().getName()).toArray(String[]::new);
    String[] expectedFiles = { logName.getName(), logName.getName() + "_recordId_" + k };
    Assert.assertArrayEquals(expectedFiles, files);
    ArchiveLogReader reader = new ArchiveHdfsLogReader(conf,
        LogServiceUtils.getArchiveLocationForLog(archiveLocation, logName));
    verifyRecords(reader, k);
    Assert.assertEquals(writer.getLastWrittenRecordId(), reader.getPosition());
    write(writer, k + 1, 2 * k);
    Assert.assertEquals(writer.getLastWrittenRecordId(), 2 * k);
    writer.close();
    reader = new ArchiveHdfsLogReader(conf,
        LogServiceUtils.getArchiveLocationForLog(archiveLocation, logName));
    verifyRecords(reader, 2 * k);

    files = ((ArchiveHdfsLogReader) reader).getFiles().stream()
        .map(fileStatus -> fileStatus.getPath().getName()).toArray(String[]::new);
    String[] expectedFiles1 =
        { logName.getName() + "_recordId_" + k, logName.getName() + "_recordId_" + 2 * k };
    Assert.assertArrayEquals(expectedFiles1, files);
    reader.close();
  }

  private void verifyRecords(ArchiveLogReader reader, int n) throws IOException {
    for (int i = 1; i <= n; i++) {
      Assert.assertEquals(i, ByteBuffer.wrap(reader.next()).getInt());
    }
    Assert.assertFalse(reader.hasNext());
    Assert.assertTrue(reader.next() == null);
    try {
      reader.readNext();
      Assert.fail();
    } catch (NoSuchElementException e) {
      //expected
    }
  }

  private void write(ArchiveLogWriter writer, int start, int end) throws IOException {
    for (Integer i = start; i <= end; i++) {
      writer.write(ByteBuffer.allocate(4).putInt(i));
    }
  }

  @Test public void testCorruptedFileEOF() throws IOException {
    FSDataOutputStream fos = FileSystem.get(conf).create(new Path(location,"testEOF"));
    fos.write(ByteBuffer.allocate(4).putInt(4).array());
    fos.write(new byte[4]);
    fos.write(ByteBuffer.allocate(4).putInt(4).array());
    // but data is just 2 bytes
    fos.write(new byte[2]);
    fos.close();
    ArchiveLogReader reader = new ArchiveHdfsLogReader(conf, location+"/testEOF");
    try {
      reader.next();
      Assert.fail();
    } catch (EOFException e) {
      //expected
    }

  }

  @Test public void testSeek() throws IOException {
    String archiveLocation = location+"/testSeek";
    LogName logName = LogName.of("testSeek");
    DistributedFileSystem fs = cluster.getFileSystem();
    fs.delete(new Path(archiveLocation), true);
    ArchiveLogWriter writer = new ArchiveHdfsLogWriter(conf);
    writer.init(archiveLocation, logName);
    int k = 100;
    write(writer, 1, k);
    writer.close();
    ArchiveLogReader reader = new ArchiveHdfsLogReader(conf,
        LogServiceUtils.getArchiveLocationForLog(archiveLocation, logName));
    reader.seek(80);
    Assert.assertEquals(80, reader.getPosition());
    int count = 0;
    while (reader.next() != null) {
      count++;
    }
    Assert.assertEquals(20, count);
  }

  @AfterClass
  public static void teardownafterclass(){
    if (cluster != null) {
      cluster.shutdown();
    }
    deleteLocalDirectory(new File(location));
  }

  static boolean deleteLocalDirectory(File dir) {
    File[] allFiles = dir.listFiles();
    if (allFiles != null) {
      for (File file : allFiles) {
        deleteLocalDirectory(file);
      }
    }
    return dir.delete();
  }

}
