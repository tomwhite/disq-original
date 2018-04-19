package com.tom_e_white.squark;

import com.tom_e_white.squark.impl.formats.vcf.AbstractVcfSink;
import com.tom_e_white.squark.impl.formats.vcf.VcfFormat;
import com.tom_e_white.squark.impl.formats.vcf.VcfSink;
import com.tom_e_white.squark.impl.formats.vcf.VcfSinkMultiple;
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

  /** An option for configuring how to write a {@link HtsjdkVariantsRdd}. */
  public interface WriteOption {}

  /** An option for configuring which format to write a {@link HtsjdkVariantsRdd} as. */
  public enum FormatWriteOption implements WriteOption {
    /** VCF format */
    VCF,
    /** block compressed VCF format (.vcf.bgz) */
    VCF_BGZ,
    /** block compressed VCF format (.vcf.gz) */
    VCF_GZ;
  }

  public enum FileCardinalityWriteOption implements WriteOption {
    /** Write a single file specified by the path. */
    SINGLE,
    /** Write multiple files in a directory specified by the path. */
    MULTIPLE
  }

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

  public void write(HtsjdkVariantsRdd htsjdkVariantsRdd, String path, WriteOption... writeOptions)
      throws IOException {
    FormatWriteOption formatWriteOption = null;
    FileCardinalityWriteOption fileCardinalityWriteOption = null;
    for (WriteOption writeOption : writeOptions) {
      if (writeOption instanceof FormatWriteOption) {
        formatWriteOption = (FormatWriteOption) writeOption;
      } else if (writeOption instanceof FileCardinalityWriteOption) {
        fileCardinalityWriteOption = (FileCardinalityWriteOption) writeOption;
      }
    }

    if (formatWriteOption == null) {
      formatWriteOption = inferFormatFromPath(path);
    }

    if (formatWriteOption == null) {
      throw new IllegalArgumentException(
          "Path does not end in VCF extension, and format not specified.");
    }

    if (fileCardinalityWriteOption == null) {
      fileCardinalityWriteOption = inferCardinalityFromPath(path);
    }

    getSink(formatWriteOption, fileCardinalityWriteOption)
        .save(sparkContext, htsjdkVariantsRdd.getHeader(), htsjdkVariantsRdd.getVariants(), path);
  }

  private FormatWriteOption inferFormatFromPath(String path) {
    VcfFormat vcfFormat = VcfFormat.fromPath(path);
    return vcfFormat == null ? null : vcfFormat.toFormatWriteOption();
  }

  private FileCardinalityWriteOption inferCardinalityFromPath(String path) {
    VcfFormat vcfFormat = VcfFormat.fromPath(path);
    return vcfFormat == null
        ? FileCardinalityWriteOption.MULTIPLE
        : FileCardinalityWriteOption.SINGLE;
  }

  private AbstractVcfSink getSink(
      FormatWriteOption formatWriteOption, FileCardinalityWriteOption fileCardinalityWriteOption) {
    switch (fileCardinalityWriteOption) {
      case SINGLE:
        return new VcfSink();
      case MULTIPLE:
        return new VcfSinkMultiple(VcfFormat.fromFormatWriteOption(formatWriteOption));
      default:
        throw new IllegalArgumentException(
            "Unrecognized cardinality: " + fileCardinalityWriteOption);
    }
  }
}
