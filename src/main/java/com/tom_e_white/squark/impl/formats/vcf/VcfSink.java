package com.tom_e_white.squark.impl.formats.vcf;

import com.google.common.collect.Iterators;
import com.tom_e_white.squark.impl.file.FileSystemWrapper;
import com.tom_e_white.squark.impl.file.HadoopFileSystemWrapper;
import com.tom_e_white.squark.impl.file.Merger;
import com.tom_e_white.squark.impl.formats.bgzf.BGZFCodec;
import htsjdk.samtools.util.BlockCompressedOutputStream;
import htsjdk.samtools.util.BlockCompressedStreamConstants;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFEncoder;
import htsjdk.variant.vcf.VCFHeader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

public class VcfSink extends AbstractVcfSink {

  private FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();

  @Override
  public void save(
      JavaSparkContext jsc, VCFHeader vcfHeader, JavaRDD<VariantContext> variants, String path)
      throws IOException {
    String shardedDir = path + ".sharded";
    Broadcast<VCFHeader> vcfHeaderBroadcast = jsc.broadcast(vcfHeader);
    JavaRDD<String> variantStrings =
        variants.mapPartitions(
            (FlatMapFunction<Iterator<VariantContext>, String>)
                variantContexts -> {
                  VCFEncoder vcfEncoder =
                      new VCFEncoder(vcfHeaderBroadcast.getValue(), false, false);
                  return Iterators.transform(variantContexts, vcfEncoder::encode);
                });
    boolean compressed = path.endsWith(BGZFCodec.DEFAULT_EXTENSION) || path.endsWith(".gz");
    if (compressed) {
      variantStrings.saveAsTextFile(shardedDir, BGZFCodec.class);
    } else {
      variantStrings.saveAsTextFile(shardedDir);
    }
    String headerFile = shardedDir + "/header" + (compressed ? BGZFCodec.DEFAULT_EXTENSION : "");
    try (OutputStream headerOut = fileSystemWrapper.create(jsc.hadoopConfiguration(), headerFile)) {
      OutputStream out = compressed ? new BlockCompressedOutputStream(headerOut, null) : headerOut;
      VariantContextWriter writer =
          new VariantContextWriterBuilder().clearOptions().setOutputVCFStream(out).build();
      writer.writeHeader(vcfHeader);
      out.flush(); // don't close BlockCompressedOutputStream since we don't want to write the
      // terminator after the header
    }
    if (compressed) {
      String terminatorFile = shardedDir + "/terminator";
      try (OutputStream out = fileSystemWrapper.create(jsc.hadoopConfiguration(), terminatorFile)) {
        out.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
      }
    }
    new Merger().mergeParts(jsc.hadoopConfiguration(), shardedDir, path);
  }
}
