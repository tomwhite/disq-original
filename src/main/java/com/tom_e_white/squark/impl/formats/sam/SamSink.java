package com.tom_e_white.squark.impl.formats.sam;

import com.tom_e_white.squark.HtsjdkReadsRdd;
import com.tom_e_white.squark.impl.file.FileSystemWrapper;
import com.tom_e_white.squark.impl.file.HadoopFileSystemWrapper;
import com.tom_e_white.squark.impl.file.Merger;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.util.AsciiWriter;
import java.io.IOException;
import java.io.Writer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Write reads to a single SAM file on Spark. This is done by writing to multiple headerless SAM
 * files in parallel, then merging the resulting files into a single SAM file.
 *
 * @see SamSource
 * @see HtsjdkReadsRdd
 */
public class SamSink extends AbstractSamSink {

  private FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();

  @Override
  public void save(
      JavaSparkContext jsc,
      SAMFileHeader header,
      JavaRDD<SAMRecord> reads,
      String path,
      String referenceSourcePath)
      throws IOException {

    String shardedDir = path + ".sharded";
    reads.map(SAMRecord::getSAMString).map(String::trim).saveAsTextFile(shardedDir);

    String headerFile = shardedDir + "/header";
    try (Writer out =
        new AsciiWriter(fileSystemWrapper.create(jsc.hadoopConfiguration(), headerFile))) {
      new SAMTextHeaderCodec().encode(out, header);
    }
    new Merger().mergeParts(jsc.hadoopConfiguration(), shardedDir, path);
  }
}
