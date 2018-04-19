package com.tom_e_white.squark.impl.formats.sam;

import com.tom_e_white.squark.HtsjdkReadsRdd;
import com.tom_e_white.squark.impl.file.NioFileSystemWrapper;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.cram.ref.CRAMReferenceSource;
import htsjdk.samtools.cram.ref.ReferenceSource;
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
public class AnySamSinkMultiple extends AbstractSamSink implements Serializable {

  private SamFormat samFormat;

  public AnySamSinkMultiple(SamFormat samFormat) {
    this.samFormat = samFormat;
  }

  @Override
  public void save(
      JavaSparkContext jsc,
      SAMFileHeader header,
      JavaRDD<SAMRecord> reads,
      String path,
      String referenceSourcePath) {

    ReferenceSource referenceSource =
        referenceSourcePath == null
            ? null
            : new ReferenceSource(NioFileSystemWrapper.asPath(referenceSourcePath));
    Broadcast<SAMFileHeader> headerBroadcast = jsc.broadcast(header);
    Broadcast<CRAMReferenceSource> referenceSourceBroadCast = jsc.broadcast(referenceSource);
    reads
        .mapPartitions(
            readIterator -> {
              AnySamOutputFormat.setHeader(headerBroadcast.getValue());
              AnySamOutputFormat.setSamFormat(samFormat);
              AnySamOutputFormat.setReferenceSource(referenceSourceBroadCast.getValue());
              return readIterator;
            })
        .mapToPair(
            (PairFunction<SAMRecord, Void, SAMRecord>) samRecord -> new Tuple2<>(null, samRecord))
        .saveAsNewAPIHadoopFile(
            path, Void.class, SAMRecord.class, AnySamOutputFormat.class, jsc.hadoopConfiguration());
  }
}
