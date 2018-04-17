package com.tom_e_white.squark.impl.formats.sam;

import com.tom_e_white.squark.HtsjdkReadsRdd;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * An output format for writing {@link SAMRecord} objects to BAM/CRAM/SAM files (including header
 * and terminator, where appropriate). This class should not be used directly.
 *
 * @see HtsjdkReadsRdd
 */
public class AnySamOutputFormat extends FileOutputFormat<Void, SAMRecord> {

  static class BamRecordWriter extends RecordWriter<Void, SAMRecord> {

    private final SAMFileWriter samFileWriter;

    public BamRecordWriter(Configuration conf, Path file, SAMFileHeader header) throws IOException {
      OutputStream out = file.getFileSystem(conf).create(file);
      SAMFileWriterFactory writerFactory = new SAMFileWriterFactory().setUseAsyncIo(false);
      samFileWriter = writerFactory.makeBAMWriter(header, true, out);
    }

    @Override
    public void write(Void ignore, SAMRecord samRecord) {
      samFileWriter.addAlignment(samRecord);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) {
      samFileWriter.close();
    }
  }

  private static SAMFileHeader header;
  private static String extension;

  public static void setHeader(SAMFileHeader samFileHeader) {
    AnySamOutputFormat.header = samFileHeader;
  }

  public static void setExtension(String extension) {
    AnySamOutputFormat.extension = extension;
  }

  @Override
  public RecordWriter<Void, SAMRecord> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException {
    Path file = getDefaultWorkFile(taskAttemptContext, extension);
    return new BamRecordWriter(taskAttemptContext.getConfiguration(), file, header);
  }
}
