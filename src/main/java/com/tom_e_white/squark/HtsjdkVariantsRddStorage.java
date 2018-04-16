package com.tom_e_white.squark;

import com.tom_e_white.squark.impl.formats.vcf.VcfSink;
import com.tom_e_white.squark.impl.formats.vcf.VcfSource;
import htsjdk.samtools.util.Locatable;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import java.io.IOException;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/** The entry point for reading or writing a {@link HtsjdkVariantsRdd}. */
public class HtsjdkVariantsRddStorage {

  private JavaSparkContext sparkContext;
  private int splitSize;

  public static HtsjdkVariantsRddStorage makeDefault(JavaSparkContext sparkContext) {
    return new HtsjdkVariantsRddStorage(sparkContext);
  }

  private HtsjdkVariantsRddStorage(JavaSparkContext sparkContext) {
    this.sparkContext = sparkContext;
  }

  public HtsjdkVariantsRddStorage splitSize(int splitSize) {
    this.splitSize = splitSize;
    return this;
  }

  public HtsjdkVariantsRdd read(String path) throws IOException {
    return read(path, null);
  }

  public <T extends Locatable> HtsjdkVariantsRdd read(String path, List<T> intervals)
      throws IOException {
    VcfSource vcfSource = new VcfSource();
    VCFHeader header = vcfSource.getFileHeader(sparkContext, path);
    JavaRDD<VariantContext> variants =
        vcfSource.getVariants(sparkContext, path, splitSize, intervals);
    return new HtsjdkVariantsRdd(header, variants);
  }

  public void write(HtsjdkVariantsRdd htsjdkVariantsRdd, String path) throws IOException {
    new VcfSink()
        .save(sparkContext, htsjdkVariantsRdd.getHeader(), htsjdkVariantsRdd.getVariants(), path);
  }
}
