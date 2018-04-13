package com.tom_e_white.squark.impl.file;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

public class Merger {

  private final FileSystemWrapper fileSystemWrapper;

  public Merger() {
    fileSystemWrapper = new HadoopFileSystemWrapper();
  }

  public void mergeParts(Configuration conf, String partDirectory, String outputFile)
      throws IOException {
    fileSystemWrapper.concat(
        conf, fileSystemWrapper.listDirectory(conf, partDirectory), outputFile);
  }
}
