package com.tom_e_white.disq.impl.formats.bam;

import com.tom_e_white.disq.HtsjdkReadsRdd;
import com.tom_e_white.disq.impl.file.FileSystemWrapper;
import com.tom_e_white.disq.impl.file.HadoopFileSystemWrapper;
import com.tom_e_white.disq.impl.file.Merger;
import com.tom_e_white.disq.impl.formats.sam.AbstractSamSink;
import htsjdk.samtools.BAMFileWriter;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.util.BlockCompressedStreamConstants;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 * Write reads to a single BAM file on Spark. This is done by writing to multiple headerless BAM
 * files in parallel, then merging the resulting files into a single BAM file.
 *
 * @see BamSource
 * @see HtsjdkReadsRdd
 */
public class BamSink extends AbstractSamSink {

  private FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();

  @Override
  public void save(
      JavaSparkContext jsc,
      SAMFileHeader header,
      JavaRDD<SAMRecord> reads,
      String path,
      String referenceSourcePath,
      String tempPartsDirectory)
      throws IOException {

    Broadcast<SAMFileHeader> headerBroadcast = jsc.broadcast(header);
    reads
        .mapPartitions(
            readIterator -> {
              HeaderlessBamOutputFormat.setHeader(headerBroadcast.getValue());
              return readIterator;
            })
        .mapToPair(
            (PairFunction<SAMRecord, Void, SAMRecord>) samRecord -> new Tuple2<>(null, samRecord))
        .saveAsNewAPIHadoopFile(
            tempPartsDirectory,
            Void.class,
            SAMRecord.class,
            HeaderlessBamOutputFormat.class,
            jsc.hadoopConfiguration());

    String headerFile = tempPartsDirectory + "/header";
    try (OutputStream out = fileSystemWrapper.create(jsc.hadoopConfiguration(), headerFile)) {
      BAMFileWriter.writeHeader(out, header);
    }

    String terminatorFile = tempPartsDirectory + "/terminator";
    try (OutputStream out = fileSystemWrapper.create(jsc.hadoopConfiguration(), terminatorFile)) {
      out.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
    }

    new Merger().mergeParts(jsc.hadoopConfiguration(), tempPartsDirectory, path);
    fileSystemWrapper.delete(jsc.hadoopConfiguration(), tempPartsDirectory);
  }
}
