package com.tom_e_white.squark.impl.formats.bam;

import com.tom_e_white.squark.HtsjdkReadsRdd;
import com.tom_e_white.squark.impl.file.FileSystemWrapper;
import com.tom_e_white.squark.impl.file.HadoopFileSystemWrapper;
import com.tom_e_white.squark.impl.file.Merger;
import com.tom_e_white.squark.impl.formats.sam.AbstractSamSink;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.util.BinaryCodec;
import htsjdk.samtools.util.BlockCompressedOutputStream;
import htsjdk.samtools.util.BlockCompressedStreamConstants;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
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
      writeHeader(header, out);
    }

    String terminatorFile = tempPartsDirectory + "/terminator";
    try (OutputStream out = fileSystemWrapper.create(jsc.hadoopConfiguration(), terminatorFile)) {
      out.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
    }

    new Merger().mergeParts(jsc.hadoopConfiguration(), tempPartsDirectory, path);
    fileSystemWrapper.delete(jsc.hadoopConfiguration(), tempPartsDirectory);
  }

  private void writeHeader(SAMFileHeader header, OutputStream out) throws IOException {
    // TODO: this is copied from htsjdk BAMFileWriter#writeHeader, which is protected.
    final StringWriter headerTextBuffer = new StringWriter();
    new SAMTextHeaderCodec().encode(headerTextBuffer, header);
    final String headerText = headerTextBuffer.toString();

    BlockCompressedOutputStream blockCompressedOutputStream =
        new BlockCompressedOutputStream(out, null);
    BinaryCodec outputBinaryCodec = new BinaryCodec(blockCompressedOutputStream);
    outputBinaryCodec.writeBytes("BAM\1".getBytes());

    // calculate and write the length of the SAM file header text and the header text
    outputBinaryCodec.writeString(headerText, true, false);

    // write the sequences binarily.  This is redundant with the text header
    outputBinaryCodec.writeInt(header.getSequenceDictionary().size());
    for (final SAMSequenceRecord sequenceRecord : header.getSequenceDictionary().getSequences()) {
      outputBinaryCodec.writeString(sequenceRecord.getSequenceName(), true, true);
      outputBinaryCodec.writeInt(sequenceRecord.getSequenceLength());
    }

    outputBinaryCodec
        .getOutputStream()
        .flush(); // don't close BlockCompressedOutputStream since we don't want to write the
    // terminator
  }
}
