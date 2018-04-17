package com.tom_e_white.squark;

import com.tom_e_white.squark.impl.formats.bam.BamSink;
import com.tom_e_white.squark.impl.formats.bam.BamSource;
import com.tom_e_white.squark.impl.formats.cram.CramSink;
import com.tom_e_white.squark.impl.formats.cram.CramSource;
import com.tom_e_white.squark.impl.formats.sam.AbstractSamSource;
import com.tom_e_white.squark.impl.formats.sam.AnySamSinkMultiple;
import com.tom_e_white.squark.impl.formats.sam.SamSink;
import com.tom_e_white.squark.impl.formats.sam.SamSource;
import htsjdk.samtools.BamFileIoUtils;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.util.IOUtil;
import htsjdk.samtools.util.Locatable;
import java.io.IOException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/** The entry point for reading or writing a {@link HtsjdkReadsRdd}. */
public class HtsjdkReadsRddStorage {

  private JavaSparkContext sparkContext;
  private int splitSize;
  private ValidationStringency validationStringency = ValidationStringency.DEFAULT_STRINGENCY;
  private boolean useNio;
  private String referenceSourcePath;

  public static HtsjdkReadsRddStorage makeDefault(JavaSparkContext sparkContext) {
    return new HtsjdkReadsRddStorage(sparkContext);
  }

  private HtsjdkReadsRddStorage(JavaSparkContext sparkContext) {
    this.sparkContext = sparkContext;
  }

  public HtsjdkReadsRddStorage splitSize(int splitSize) {
    this.splitSize = splitSize;
    return this;
  }

  public HtsjdkReadsRddStorage validationStringency(ValidationStringency validationStringency) {
    this.validationStringency = validationStringency;
    return this;
  }

  public HtsjdkReadsRddStorage useNio(boolean useNio) {
    this.useNio = useNio;
    return this;
  }

  public HtsjdkReadsRddStorage referenceSourcePath(String referenceSourcePath) {
    this.referenceSourcePath = referenceSourcePath;
    return this;
  }

  public HtsjdkReadsRdd read(String path) throws IOException {
    return read(path, null);
  }

  public <T extends Locatable> HtsjdkReadsRdd read(
      String path, HtsjdkReadsTraversalParameters<T> traversalParameters) throws IOException {
    AbstractSamSource abstractSamSource;

    if (path.endsWith(CramIO.CRAM_FILE_EXTENSION)) {
      abstractSamSource = new CramSource();
    } else if (path.endsWith(IOUtil.SAM_FILE_EXTENSION)) {
      abstractSamSource = new SamSource();
    } else {
      abstractSamSource = new BamSource(useNio);
    }
    SAMFileHeader header =
        abstractSamSource.getFileHeader(
            sparkContext, path, validationStringency, referenceSourcePath);
    JavaRDD<SAMRecord> reads =
        abstractSamSource.getReads(
            sparkContext,
            path,
            splitSize,
            traversalParameters,
            validationStringency,
            referenceSourcePath);
    return new HtsjdkReadsRdd(header, reads);
  }

  public void write(HtsjdkReadsRdd htsjdkReadsRdd, String path) throws IOException {
    if (path.endsWith(CramIO.CRAM_FILE_EXTENSION)) {
      new CramSink()
          .save(
              sparkContext,
              htsjdkReadsRdd.getHeader(),
              htsjdkReadsRdd.getReads(),
              path,
              referenceSourcePath);
    } else if (path.endsWith(IOUtil.SAM_FILE_EXTENSION)) {
      new SamSink().save(sparkContext, htsjdkReadsRdd.getHeader(), htsjdkReadsRdd.getReads(), path);
    } else if (path.endsWith(".bams") || path.endsWith(".bams/")) {
      new AnySamSinkMultiple(BamFileIoUtils.BAM_FILE_EXTENSION)
          .save(sparkContext, htsjdkReadsRdd.getHeader(), htsjdkReadsRdd.getReads(), path);
    } else if (path.endsWith(".crams") || path.endsWith(".crams/")) {
      new AnySamSinkMultiple(CramIO.CRAM_FILE_EXTENSION)
          .save(sparkContext, htsjdkReadsRdd.getHeader(), htsjdkReadsRdd.getReads(), path);
    } else if (path.endsWith(".sams") || path.endsWith(".sams/")) {
      new AnySamSinkMultiple(IOUtil.SAM_FILE_EXTENSION)
          .save(sparkContext, htsjdkReadsRdd.getHeader(), htsjdkReadsRdd.getReads(), path);
    } else {
      new BamSink().save(sparkContext, htsjdkReadsRdd.getHeader(), htsjdkReadsRdd.getReads(), path);
    }
  }
}
