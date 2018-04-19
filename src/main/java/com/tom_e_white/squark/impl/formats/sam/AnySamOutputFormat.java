package com.tom_e_white.squark.impl.formats.sam;

import com.tom_e_white.squark.HtsjdkReadsRdd;
import htsjdk.samtools.BamFileIoUtils;
import htsjdk.samtools.CRAMFileWriter;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.ref.CRAMReferenceSource;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.util.IOUtil;
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

    public BamRecordWriter(Configuration conf, Path file, SAMFileHeader header, String extension, CRAMReferenceSource refSource) throws IOException {
      OutputStream out = file.getFileSystem(conf).create(file);
      SAMFileWriterFactory writerFactory = new SAMFileWriterFactory().setUseAsyncIo(false);
      if (extension.equals(BamFileIoUtils.BAM_FILE_EXTENSION)) {
        samFileWriter = writerFactory.makeBAMWriter(header, true, out);
      } else if (extension.equals(CramIO.CRAM_FILE_EXTENSION)) {
        samFileWriter = new CRAMFileWriter(out, refSource, header, null);
      } else if (extension.equals(IOUtil.SAM_FILE_EXTENSION)) {
        samFileWriter = writerFactory.makeSAMWriter(header, true, out);
      } else {
        throw new IllegalArgumentException("Unrecognized extension: " + extension);
      }
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
  private static CRAMReferenceSource refSource;

  public static void setHeader(SAMFileHeader samFileHeader) {
    AnySamOutputFormat.header = samFileHeader;
  }

  public static void setExtension(String extension) {
    AnySamOutputFormat.extension = extension;
  }

  public static void setReferenceSource(CRAMReferenceSource referenceSource) {
    AnySamOutputFormat.refSource = referenceSource;
  }

  @Override
  public RecordWriter<Void, SAMRecord> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException {
    Path file = getDefaultWorkFile(taskAttemptContext, extension);
    return new BamRecordWriter(taskAttemptContext.getConfiguration(), file, header, extension, refSource);
  }
}
