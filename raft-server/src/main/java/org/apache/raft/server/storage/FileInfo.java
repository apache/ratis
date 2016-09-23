package org.apache.raft.server.storage;

import org.apache.hadoop.io.MD5Hash;

import java.nio.file.Path;

/**
 * Metadata about a file.
 */
public class FileInfo {
  private Path path;
  private MD5Hash fileDigest;
  private long fileSize;

  public FileInfo(Path path, MD5Hash fileDigest) {
    this.path = path;
    this.fileDigest = fileDigest;
    this.fileSize = path.toFile().length();
  }

  @Override
  public String toString() {
    return path.toString();
  }

  public Path getPath() {
    return path;
  }

  public MD5Hash getFileDigest() {
    return fileDigest;
  }

  public long getFileSize() {
    return fileSize;
  }
}
