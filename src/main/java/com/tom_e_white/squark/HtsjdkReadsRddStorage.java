package com.tom_e_white.squark;

import com.tom_e_white.squark.impl.formats.bam.BamSink;
import com.tom_e_white.squark.impl.formats.bam.BamSource;
import com.tom_e_white.squark.impl.formats.cram.CramSink;
import com.tom_e_white.squark.impl.formats.cram.CramSource;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.cram.build.CramIO;
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
    if (path.endsWith(CramIO.CRAM_FILE_EXTENSION)) {
      CramSource cramSource = new CramSource();
      SAMFileHeader header =
          cramSource.getFileHeader(sparkContext, path, validationStringency, referenceSourcePath);
      JavaRDD<SAMRecord> reads =
          cramSource.getReads(
              sparkContext,
              path,
              splitSize,
              traversalParameters,
              validationStringency,
              referenceSourcePath);
      return new HtsjdkReadsRdd(header, reads);
    } else {
      BamSource bamSource = new BamSource(useNio);
      SAMFileHeader header = bamSource.getFileHeader(sparkContext, path, validationStringency);
      JavaRDD<SAMRecord> reads =
          bamSource.getReads(
              sparkContext, path, splitSize, traversalParameters, validationStringency);
      return new HtsjdkReadsRdd(header, reads);
    }
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
    } else {
      new BamSink().save(sparkContext, htsjdkReadsRdd.getHeader(), htsjdkReadsRdd.getReads(), path);
    }
  }
}
