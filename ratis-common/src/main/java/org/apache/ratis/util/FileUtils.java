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
package org.apache.ratis.util;

import org.apache.ratis.io.nativeio.NativeIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;

public class FileUtils {
  public static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

  public static void truncateFile(File f, long target) throws IOException {
    try (FileOutputStream out = new FileOutputStream(f, true)) {
      out.getChannel().truncate(target);
    }
  }

  public static void deleteFile(File f) throws IOException {
    try {
      Files.delete(f.toPath());
    } catch (IOException e) {
      LOG.warn("Could not delete " + f);
      throw e;
    }
  }

  /**
   * Delete a directory and all its contents.  If
   * we return false, the directory may be partially-deleted.
   * (1) If dir is symlink to a file, the symlink is deleted. The file pointed
   *     to by the symlink is not deleted.
   * (2) If dir is symlink to a directory, symlink is deleted. The directory
   *     pointed to by symlink is not deleted.
   * (3) If dir is a normal file, it is deleted.
   * (4) If dir is a normal directory, then dir and all its contents recursively
   *     are deleted.
   */
  public static boolean fullyDelete(final File dir) {
    if (deleteImpl(dir, false)) {
      // dir is (a) normal file, (b) symlink to a file, (c) empty directory or
      // (d) symlink to a directory
      return true;
    }
    // handle nonempty directory deletion
    return fullyDeleteContents(dir) && deleteImpl(dir, true);
  }

  private static boolean deleteImpl(final File f, final boolean doLog) {
    if (f == null) {
      LOG.warn("null file argument.");
      return false;
    }
    final boolean wasDeleted = f.delete();
    if (wasDeleted) {
      return true;
    }
    final boolean ex = f.exists();
    if (doLog && ex) {
      LOG.warn("Failed to delete file or dir ["
          + f.getAbsolutePath() + "]: it still exists.");
    }
    return !ex;
  }

  /**
   * Delete the contents of a directory, not the directory itself.  If
   * we return false, the directory may be partially-deleted.
   * If dir is a symlink to a directory, all the contents of the actual
   * directory pointed to by dir will be deleted.
   */
  private static boolean fullyDeleteContents(final File dir) {
    boolean deletionSucceeded = true;
    final File[] contents = dir.listFiles();
    if (contents != null) {
      for (File content : contents) {
        if (content.isFile()) {
          if (!deleteImpl(content, true)) {
            deletionSucceeded = false;
          }
        } else {
          // Either directory or symlink to another directory.
          // Try deleting the directory as this might be a symlink
          if (deleteImpl(content, false)) {
            // this was indeed a symlink or an empty directory
            continue;
          }
          // if not an empty directory or symlink let
          // fullyDelete handle it.
          if (!fullyDelete(content)) {
            deletionSucceeded = false;
            // continue deletion of other files/dirs under dir
          }
        }
      }
    }
    return deletionSucceeded;
  }

  /**
   * Interprets the passed string as a URI. In case of error it
   * assumes the specified string is a file.
   *
   * @param s the string to interpret
   * @return the resulting URI
   */
  public static URI stringAsURI(String s) throws IOException {
    URI u = null;
    // try to make a URI
    try {
      u = new URI(s);
    } catch (URISyntaxException e){
      LOG.error("Syntax error in URI " + s
          + ". Please check hdfs configuration.", e);
    }

    // if URI is null or scheme is undefined, then assume it's file://
    if(u == null || u.getScheme() == null){
      LOG.warn("Path " + s + " should be specified as a URI "
          + "in configuration files. Please update configuration.");
      u = fileAsURI(new File(s));
    }
    return u;
  }

  /**
   * Converts the passed File to a URI. This method trims the trailing slash if
   * one is appended because the underlying file is in fact a directory that
   * exists.
   *
   * @param f the file to convert
   * @return the resulting URI
   */
  public static URI fileAsURI(File f) throws IOException {
    URI u = f.getCanonicalFile().toURI();

    // trim the trailing slash, if it's present
    if (u.getPath().endsWith("/")) {
      String uriAsString = u.toString();
      try {
        u = new URI(uriAsString.substring(0, uriAsString.length() - 1));
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
    }
    return u;
  }

  /**
   * A wrapper for {@link File#listFiles()}. This java.io API returns null
   * when a dir is not a directory or for any I/O error. Instead of having
   * null check everywhere File#listFiles() is used, we will add utility API
   * to get around this problem. For the majority of cases where we prefer
   * an IOException to be thrown.
   * @param dir directory for which listing should be performed
   * @return list of files or empty list
   * @exception IOException for invalid directory or for a bad disk.
   */
  public static File[] listFiles(File dir) throws IOException {
    File[] files = dir.listFiles();
    if(files == null) {
      throw new IOException("Invalid directory or I/O error occurred for dir: "
          + dir.toString());
    }
    return files;
  }

  /**
   * Platform independent implementation for {@link File#canWrite()}
   * @param f input file
   * @return On Unix, same as {@link File#canWrite()}
   *         On Windows, true if process has write access on the path
   */
  public static boolean canWrite(File f) {
    if (PlatformUtils.WINDOWS) {
      try {
        return NativeIO.Windows.access(f.getCanonicalPath(),
            NativeIO.Windows.AccessRight.ACCESS_WRITE);
      } catch (IOException e) {
        return false;
      }
    } else {
      return f.canWrite();
    }
  }
}
