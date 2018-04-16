package com.tom_e_white.squark.impl.formats.sam;

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

public class SamSink {

  private FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();

  public void save(
      JavaSparkContext jsc, SAMFileHeader header, JavaRDD<SAMRecord> reads, String path)
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
