package org.apache.ratis.logservice.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.ratis.logservice.api.ArchiveLogWriter;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.util.LogServiceUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestArchiveHdfsLogReaderAndWriter {
  static MiniDFSCluster cluster;
  static Configuration conf;

  @BeforeClass
  public static void setup() throws IOException {
     conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
  }

  @Test
  public void testRollingWriter() throws IOException {
    String archiveLocation = "/logs/archive/testRollingWriter";
    LogName logName = LogName.of("testRollingWriterLogName");
    ArchiveLogWriter writer = new ArchiveHdfsLogWriter(conf);
    writer.init(archiveLocation,logName);
    ByteBuffer testMessage =  ByteBuffer.wrap("Hello world!".getBytes());
    writer.write(testMessage);
    writer.write(testMessage);
    writer.rollWriter();
    DistributedFileSystem fs = cluster.getFileSystem();
    String[] files = Arrays.stream(
        fs.listStatus(new Path(LogServiceUtils.getArchiveLocationForLog(archiveLocation, logName))))
        .map(fileStatus -> fileStatus.getPath().getName()).toArray(String[]::new);
    String[] expectedFiles= { logName.getName(),
        logName.getName()+"_recordId_"
            + "2" };
    Assert.assertArrayEquals(expectedFiles,files);
  }

  @Test
  public void testReaderFileSorting() throws IOException {

  }

  @Test
  public void testWriteAndRead() throws IOException {

  }


}
