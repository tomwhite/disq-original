package com.tom_e_white.squark.impl.formats.cram;

import com.tom_e_white.squark.HtsjdkReadsRdd;
import com.tom_e_white.squark.impl.file.FileSystemWrapper;
import com.tom_e_white.squark.impl.file.HadoopFileSystemWrapper;
import com.tom_e_white.squark.impl.file.Merger;
import com.tom_e_white.squark.impl.file.NioFileSystemWrapper;
import com.tom_e_white.squark.impl.formats.sam.AbstractSamSink;
import htsjdk.samtools.CRAMContainerStreamWriter;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.cram.common.CramVersions;
import htsjdk.samtools.cram.ref.CRAMReferenceSource;
import htsjdk.samtools.cram.ref.ReferenceSource;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 * Write reads to a single CRAM file on Spark. This is done by writing to multiple headerless CRAM
 * files in parallel, then merging the resulting files into a single CRAM file.
 *
 * @see CramSource
 * @see HtsjdkReadsRdd
 */
public class CramSink extends AbstractSamSink {

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

    ReferenceSource referenceSource =
        new ReferenceSource(NioFileSystemWrapper.asPath(referenceSourcePath));
    Broadcast<SAMFileHeader> headerBroadcast = jsc.broadcast(header);
    Broadcast<CRAMReferenceSource> referenceSourceBroadCast = jsc.broadcast(referenceSource);
    reads
        .mapPartitions(
            readIterator -> {
              CramOutputFormat.setHeader(headerBroadcast.getValue());
              CramOutputFormat.setReferenceSource(referenceSourceBroadCast.getValue());
              return readIterator;
            })
        .mapToPair(
            (PairFunction<SAMRecord, Void, SAMRecord>) samRecord -> new Tuple2<>(null, samRecord))
        .saveAsNewAPIHadoopFile(
            tempPartsDirectory,
            Void.class,
            SAMRecord.class,
            CramOutputFormat.class,
            jsc.hadoopConfiguration());

    String headerFile = tempPartsDirectory + "/header";
    try (OutputStream out = fileSystemWrapper.create(jsc.hadoopConfiguration(), headerFile)) {
      writeHeader(header, out, headerFile, referenceSource);
    }

    String terminatorFile = tempPartsDirectory + "/terminator";
    try (OutputStream out = fileSystemWrapper.create(jsc.hadoopConfiguration(), terminatorFile)) {
      CramIO.issueEOF(CramVersions.DEFAULT_CRAM_VERSION, out);
    }

    new Merger().mergeParts(jsc.hadoopConfiguration(), tempPartsDirectory, path);
  }

  private void writeHeader(
      SAMFileHeader header, OutputStream out, String headerFile, ReferenceSource referenceSource) {
    CRAMContainerStreamWriter cramWriter =
        new CRAMContainerStreamWriter(out, null, referenceSource, header, headerFile);
    cramWriter.writeHeader(header);
    cramWriter.finish(false); // don't write terminator
  }
}
