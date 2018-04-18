package com.tom_e_white.squark;

import htsjdk.samtools.util.Locatable;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;

public class BcftoolsTestUtil {

  private static final String BCFTOOLS_BIN_PROPERTY = "squark.bcftools.bin";

  public static boolean isBcftoolsAvailable() {
    String bin = getBcftoolsBin();
    if (bin == null) {
      return false;
    }
    File binFile = new File(bin);
    if (!binFile.exists()) {
      throw new IllegalArgumentException(
          String.format(
              "%s property is set to non-existent file: %s", BCFTOOLS_BIN_PROPERTY, binFile));
    }
    return true;
  }

  private static String getBcftoolsBin() {
    return System.getProperty(BCFTOOLS_BIN_PROPERTY);
  }

  public static <T extends Locatable> int countVariants(File file) throws IOException {
    return countVariants(file, null);
  }

  public static <T extends Locatable> int countVariants(File file, T interval) throws IOException {
    CommandLine commandLine = new CommandLine(getBcftoolsBin());
    commandLine.addArgument("view");
    commandLine.addArgument("-H"); // no header
    commandLine.addArgument(file.getAbsolutePath());
    if (interval != null) {
      commandLine.addArgument(
          String.format("%s:%s-%s", interval.getContig(), interval.getStart(), interval.getEnd()));
    }
    System.out.println(commandLine);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ByteArrayOutputStream errorStream = new ByteArrayOutputStream();

    DefaultExecutor exec = new DefaultExecutor();
    PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream, errorStream);
    exec.setStreamHandler(streamHandler);
    int rc = exec.execute(commandLine);
    String result =
        outputStream
            .toString()
            .trim(); // the actual variants, one per line (bcftools doesn't have a count option)
    String error = errorStream.toString();

    if (rc != 0) {
      throw new IllegalStateException(
          String.format(
              "Bcftools failed processing file %s with code %s. Stderr: %s", file, rc, error));
    }
    if (error.length() > 0) {
      System.err.println(
          String.format(
              "Bcftools produced stderr while processing file %s. Stderr: %s", file, error));
    }
    return result.split("\r\n|\r|\n").length;
  }
}
