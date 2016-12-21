
package org.apache.raft.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class FileUtils {
  public static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

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
}
