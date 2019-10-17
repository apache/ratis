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

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ratis.logservice.api.ArchiveLogReader;
import org.apache.ratis.logservice.util.LogServiceUtils;
import org.apache.ratis.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArchiveHdfsLogReader implements ArchiveLogReader {
  public static final Logger LOG = LoggerFactory.getLogger(ArchiveHdfsLogReader.class);
  private long fileLength;
  private List<FileStatus> files;
  private FileSystem hdfs;
  private FSDataInputStream is;
  private byte[] currentRecord;
  private int fileCounter = 0;
  private int currentRecordId;

  public ArchiveHdfsLogReader(String archiveLocation) throws IOException {
    this(new Configuration(), archiveLocation);
  }

  public ArchiveHdfsLogReader(Configuration configuration, String archiveLocation)
      throws IOException {
    this.hdfs = FileSystem.get(configuration);
    Path archiveLocationPath = new Path(archiveLocation);
    if (!hdfs.exists(archiveLocationPath)) {
      throw new FileNotFoundException(archiveLocation);
    }
    files = Arrays.asList(hdfs.listStatus(archiveLocationPath));
    if (files.size() > 0) {
      Collections.sort(files, new Comparator<FileStatus>() {
        @Override public int compare(FileStatus o1, FileStatus o2) {
          //ascending order
          //currently written file (without _recordId_) will be sorted at the last
          return LogServiceUtils.getRecordIdFromRolledArchiveFile(o1.getPath())
              .compareTo(LogServiceUtils.getRecordIdFromRolledArchiveFile(o2.getPath()));
        }
      });
      openNextFilePath();
      loadNext();
    }
  }

  private Path openNextFilePath() throws IOException {
    Path filePath = files.get(fileCounter).getPath();
    this.is = this.hdfs.open(filePath);
    this.fileLength = this.hdfs.getFileStatus(filePath).getLen();
    fileCounter++;
    return filePath;

  }

  @Override public void seek(long recordId) throws IOException {
    while (currentRecordId < recordId && hasNext()) {
      next();
    }
  }

  @Override public boolean hasNext() throws IOException {
    return currentRecord != null;
  }

  @Override public byte[] next() throws IOException {
    byte[] current = currentRecord;
    currentRecord = null;
    if (current != null) {
      currentRecordId++;
    }
    loadNext();
    return current;
  }

  @Override public long getCurrentRaftIndex() {
    throw new UnsupportedOperationException(
        "getCurrentRaftIndex() is not supported for archive hdfs log reader");
  }

  @Override public ByteBuffer readNext() throws IOException {
    byte[] current = next();
    if (current == null) {
      throw new NoSuchElementException();
    }
    return ByteBuffer.wrap(current);
  }

  private int readLength() throws IOException {
    int length;
    try {
      length = is.readInt();
    } catch (EOFException e) {
      if (files.size() <= fileCounter) {
        LOG.trace("EOF and no more file to read, throwing back", e);
        throw e;
      } else {
        LOG.trace("EOF.. Opening next file: {}!!", files.get(fileCounter).getPath());
        openNextFilePath();
        length = is.readInt();
      }
    }
    return length;
  }

  @Override public void readNext(ByteBuffer buffer) throws IOException {
    Preconditions.checkNotNull(buffer, "buffer is NULL");
    byte[] current = next();
    if (current == null) {
      throw new NoSuchElementException();
    }
    buffer.put(current);
  }


  @Override public List<ByteBuffer> readBulk(int numRecords) throws IOException {
    Preconditions.checkArgument(numRecords > 0, "number of records must be greater than 0");
    List<ByteBuffer> ret = new ArrayList<ByteBuffer>();
    try {

      for (int i = 0; i < numRecords; i++) {
        ByteBuffer buffer = readNext();
        ret.add(buffer);
      }

    } catch (EOFException eof) {
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      return ret;
    }
  }

  @Override public int readBulk(ByteBuffer[] buffers) throws IOException {
    Preconditions.checkNotNull(buffers, "list of buffers is NULL");
    Preconditions.checkArgument(buffers.length > 0, "list of buffers is empty");
    int count = 0;
    try {
      for (int i = 0; i < buffers.length; i++) {
        readNext(buffers[i]);
        count++;
      }
    } catch (EOFException eof) {

    }
    return count;
  }

  @Override public long getPosition() throws IOException {
    return currentRecordId;
  }

  @Override public void close() throws IOException {
    if (this.is != null) {
      this.is.close();
      this.is = null;
    }
  }

  private void loadNext() throws IOException {
    int length;
    try {
      length = readLength();
    } catch (EOFException e) {
      currentRecord = null;
      return;
    }
    byte[] bytes = new byte[length];
    if (is.read(bytes) != length) {
      throw new EOFException(
          "File seems to be corrupted, Encountered EOF before reading the complete record");
    }
    currentRecord = bytes;
  }

  //Only for testing
  public List<FileStatus> getFiles(){
    return files;
  }

}
