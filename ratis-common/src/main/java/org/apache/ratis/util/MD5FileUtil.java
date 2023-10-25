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
package org.apache.ratis.util;

import org.apache.ratis.io.MD5Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class MD5FileUtil {
  public static final Logger LOG = LoggerFactory.getLogger(MD5FileUtil.class);

  // TODO: we should provide something like Hadoop's checksum fs for the local filesystem
  // so that individual state machines do not have to deal with checksumming/corruption prevention.
  // Keep the checksum and data in the same block format instead of individual files.

  public static final String MD5_SUFFIX = ".md5";
  private static final String LINE_REGEX = "([0-9a-f]{32}) [ *](.+)";
  private static final Pattern LINE_PATTERN = Pattern.compile(LINE_REGEX);

  static Matcher getMatcher(String md5) {
    return Optional.ofNullable(md5)
        .map(LINE_PATTERN::matcher)
        .filter(Matcher::matches)
        .orElse(null);
  }

  static String getDoesNotMatchString(String line) {
    return "\"" + line + "\" does not match the pattern " + LINE_REGEX;
  }

  /**
   * Verify that the previously saved md5 for the given file matches
   * expectedMd5.
   */
  public static void verifySavedMD5(File dataFile, MD5Hash expectedMD5)
      throws IOException {
    MD5Hash storedHash = readStoredMd5ForFile(dataFile);
    // Check the hash itself
    if (!expectedMD5.equals(storedHash)) {
      throw new IOException(
          "File " + dataFile + " did not match stored MD5 checksum " +
              " (stored: " + storedHash + ", computed: " + expectedMD5);
    }
  }

  /** Read the first line of the given file. */
  private static String readFirstLine(File f) throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        FileUtils.newInputStream(f), StandardCharsets.UTF_8))) {
      return Optional.ofNullable(reader.readLine()).map(String::trim).orElse(null);
    } catch (IOException ioe) {
      throw new IOException("Failed to read file: " + f, ioe);
    }
  }

  /**
   * Read the md5 checksum stored alongside the given data file.
   * @param dataFile the file containing data
   * @return the checksum stored in dataFile.md5
   */
  public static MD5Hash readStoredMd5ForFile(File dataFile) throws IOException {
    final File md5File = getDigestFileForFile(dataFile);
    if (!md5File.exists()) {
      return null;
    }

    final String md5 = readFirstLine(md5File);
    final Matcher matcher = Optional.ofNullable(getMatcher(md5)).orElseThrow(() -> new IOException(
        "Invalid MD5 file " + md5File + ": the content " + getDoesNotMatchString(md5)));
    String storedHash = matcher.group(1);
    File referencedFile = new File(matcher.group(2));

    // Sanity check: Make sure that the file referenced in the .md5 file at
    // least has the same name as the file we expect
    if (!referencedFile.getName().equals(dataFile.getName())) {
      throw new IOException(
          "MD5 file at " + md5File + " references file named " +
              referencedFile.getName() + " but we expected it to reference " +
              dataFile);
    }
    return new MD5Hash(storedHash);
  }

  /**
   * Read dataFile and compute its MD5 checksum.
   */
  public static MD5Hash computeMd5ForFile(File dataFile) throws IOException {
    final int bufferSize = SizeInBytes.ONE_MB.getSizeInt();
    final MessageDigest digester = MD5Hash.getDigester();
    try (FileChannel in = FileUtils.newFileChannel(dataFile, StandardOpenOption.READ)) {
      final long fileSize = in.size();
      for (int offset = 0; offset < fileSize; ) {
        final int readSize = Math.toIntExact(Math.min(fileSize - offset, bufferSize));
        digester.update(in.map(FileChannel.MapMode.READ_ONLY, offset, readSize));
        offset += readSize;
      }
    }
    return new MD5Hash(digester.digest());
  }

  public static MD5Hash computeAndSaveMd5ForFile(File dataFile) {
    final MD5Hash md5;
    try {
      md5 = computeMd5ForFile(dataFile);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to compute MD5 for file " + dataFile, e);
    }
    try {
      saveMD5File(dataFile, md5);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to save MD5 " + md5 + " for file " + dataFile, e);
    }
    return md5;
  }

  /**
   * Save the ".md5" file that lists the md5sum of another file.
   * @param dataFile the original file whose md5 was computed
   * @param digest the computed digest
   */
  public static void saveMD5File(File dataFile, MD5Hash digest)
      throws IOException {
    final String digestString = StringUtils.bytes2HexString(digest.getDigest());
    saveMD5File(dataFile, digestString);
  }

  private static void saveMD5File(File dataFile, String digestString)
      throws IOException {
    final String md5Line = digestString + " *" + dataFile.getName() + "\n";
    if (getMatcher(md5Line.trim()) == null) {
      throw new IllegalArgumentException("Invalid md5 string: " + getDoesNotMatchString(digestString));
    }

    final File md5File = getDigestFileForFile(dataFile);
    try (AtomicFileOutputStream afos = new AtomicFileOutputStream(md5File)) {
      afos.write(md5Line.getBytes(StandardCharsets.UTF_8));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Saved MD5 " + digestString + " to " + md5File);
    }
  }

  /**
   * @return a reference to the file with .md5 suffix that will
   * contain the md5 checksum for the given data file.
   */
  public static File getDigestFileForFile(File file) {
    return new File(file.getParentFile(), file.getName() + MD5_SUFFIX);
  }
}
