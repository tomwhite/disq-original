package com.tom_e_white.squark;

import htsjdk.samtools.util.Locatable;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;

public class SamtoolsTestUtil {

  private static final String SAMTOOLS_BIN_PROPERTY = "squark.samtools.bin";

  public static boolean isSamtoolsAvailable() {
    String bin = getSamtoolsBin();
    if (bin == null) {
      return false;
    }
    File binFile = new File(bin);
    if (!binFile.exists()) {
      throw new IllegalArgumentException(
          String.format(
              "%s property is set to non-existent file: %s", SAMTOOLS_BIN_PROPERTY, binFile));
    }
    return true;
  }

  private static String getSamtoolsBin() {
    return System.getProperty(SAMTOOLS_BIN_PROPERTY);
  }

  public static <T extends Locatable> int countReads(File file) throws IOException {
    return countReads(file, null, null);
  }

  public static <T extends Locatable> int countReads(File file, File reference) throws IOException {
    return countReads(file, reference, null);
  }

  public static <T extends Locatable> int countReads(
      File file, File reference, HtsjdkReadsTraversalParameters<T> traversalParameters)
      throws IOException {
    CommandLine commandLine = new CommandLine(getSamtoolsBin());
    commandLine.addArgument("view");
    commandLine.addArgument("-c"); // count
    if (reference != null) {
      commandLine.addArgument("-T");
      commandLine.addArgument(reference.getAbsolutePath());
    }
    commandLine.addArgument(file.getAbsolutePath());
    if (traversalParameters != null) {
      if (traversalParameters.getIntervalsForTraversal() != null) {
        for (T locatable : traversalParameters.getIntervalsForTraversal()) {
          commandLine.addArgument(
              String.format(
                  "%s:%s-%s", locatable.getContig(), locatable.getStart(), locatable.getEnd()));
        }
      }
      if (traversalParameters.getTraverseUnplacedUnmapped()) {
        commandLine.addArgument("*");
      }
    }

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ByteArrayOutputStream errorStream = new ByteArrayOutputStream();

    DefaultExecutor exec = new DefaultExecutor();
    PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream, errorStream);
    exec.setStreamHandler(streamHandler);
    int rc = exec.execute(commandLine);
    String result = outputStream.toString().trim();
    String error = errorStream.toString();

    if (rc != 0) {
      throw new IllegalStateException(
          String.format(
              "Samtools failed processing file %s with code %s. Stderr: %s", file, rc, error));
    }
    if (error.length() > 0) {
      System.err.println(
          String.format(
              "Samtools produced stderr while processing file %s. Stderr: %s", file, error));
    }
    return Integer.parseInt(result);
  }
}
