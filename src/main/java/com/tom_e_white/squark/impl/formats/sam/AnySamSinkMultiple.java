package com.tom_e_white.squark.impl.formats.sam;

import com.tom_e_white.squark.HtsjdkReadsRdd;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import java.io.Serializable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 * Write reads to multiple BAM/CRAM/SAM files in a directory on Spark. This is more efficient than
 * {@link com.tom_e_white.squark.impl.formats.bam.BamSink}, {@link
 * com.tom_e_white.squark.impl.formats.cram.CramSink}, and {@link SamSink} since it avoids the cost
 * of merging the headerless files at the end, however it multiple files may not be as easy to
 * consume for some external systems.
 *
 * @see com.tom_e_white.squark.impl.formats.bam.BamSink
 * @see com.tom_e_white.squark.impl.formats.cram.CramSink
 * @see SamSink
 * @see HtsjdkReadsRdd
 */
public class AnySamSinkMultiple implements Serializable {

  private String extension;

  public AnySamSinkMultiple(String extension) {
    this.extension = extension;
  }

  public void save(
      JavaSparkContext jsc, SAMFileHeader header, JavaRDD<SAMRecord> reads, String path) {

    Broadcast<SAMFileHeader> headerBroadcast = jsc.broadcast(header);
    reads
        .mapPartitions(
            readIterator -> {
              AnySamOutputFormat.setHeader(headerBroadcast.getValue());
              AnySamOutputFormat.setExtension(extension);
              return readIterator;
            })
        .mapToPair(
            (PairFunction<SAMRecord, Void, SAMRecord>) samRecord -> new Tuple2<>(null, samRecord))
        .saveAsNewAPIHadoopFile(
            path, Void.class, SAMRecord.class, AnySamOutputFormat.class, jsc.hadoopConfiguration());
  }
}
