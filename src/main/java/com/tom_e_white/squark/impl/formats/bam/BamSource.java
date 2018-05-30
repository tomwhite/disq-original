package com.tom_e_white.squark.impl.formats.bam;

import com.google.common.collect.Iterators;
import com.tom_e_white.squark.HtsjdkReadsRdd;
import com.tom_e_white.squark.HtsjdkReadsTraversalParameters;
import com.tom_e_white.squark.impl.file.HadoopFileSystemWrapper;
import com.tom_e_white.squark.impl.file.NioFileSystemWrapper;
import com.tom_e_white.squark.impl.file.PathChunk;
import com.tom_e_white.squark.impl.formats.AutocloseIteratorWrapper;
import com.tom_e_white.squark.impl.formats.BoundedTraversalUtil;
import com.tom_e_white.squark.impl.formats.SerializableHadoopConfiguration;
import com.tom_e_white.squark.impl.formats.bgzf.BgzfBlockGuesser.BgzfBlock;
import com.tom_e_white.squark.impl.formats.bgzf.BgzfBlockSource;
import com.tom_e_white.squark.impl.formats.bgzf.BgzfVirtualFilePointerUtil;
import com.tom_e_white.squark.impl.formats.sam.AbstractSamSource;
import com.tom_e_white.squark.impl.formats.sam.SamFormat;
import htsjdk.samtools.AbstractBAMFileIndex;
import htsjdk.samtools.BAMFileReader;
import htsjdk.samtools.BAMFileSpan;
import htsjdk.samtools.BAMIndex;
import htsjdk.samtools.Chunk;
import htsjdk.samtools.ExtSeekableBufferedStream;
import htsjdk.samtools.QueryInterval;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReader.PrimitiveSamReaderToSamReaderAdapter;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.Locatable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

/**
 * Load reads from a BAM file on Spark.
 *
 * @see BamSink
 * @see HtsjdkReadsRdd
 */
public class BamSource extends AbstractSamSource implements Serializable {

  private static final int MAX_READ_SIZE = 10_000_000;

  private final BgzfBlockSource bgzfBlockSource;

  public BamSource() {
    this(false);
  }

  /**
   * @param useNio if true use the NIO filesystem APIs rather than the Hadoop filesystem APIs. This
   *     is appropriate for cloud stores where file locality is not relied upon.
   */
  public BamSource(boolean useNio) {
    super(useNio ? new NioFileSystemWrapper() : new HadoopFileSystemWrapper());
    this.bgzfBlockSource = new BgzfBlockSource(useNio);
  }

  @Override
  public SamFormat getSamFormat() {
    return SamFormat.BAM;
  }

  /**
   * @return the {@link PathChunk} for the partition, or null if there is none (e.g. in the case of
   *     long reads, and/or very small partitions).
   */
  private <T extends Locatable> PathChunk getFirstReadInPartition(
      Configuration conf,
      Iterator<BgzfBlock> bgzfBlocks,
      ValidationStringency stringency,
      String referenceSourcePath)
      throws IOException {
    PathChunk pathChunk = null;
    BamRecordGuesser bamRecordGuesser = null;
    try {
      String partitionPath = null;
      int index = 0; // limit search to MAX_READ_SIZE positions
      while (bgzfBlocks.hasNext()) {
        BgzfBlock block = bgzfBlocks.next();
        if (partitionPath == null) { // assume each partition comes from only a single file path
          partitionPath = block.path;
          try (SamReader samReader =
              createSamReader(conf, partitionPath, stringency, referenceSourcePath)) {
            SAMFileHeader header = samReader.getFileHeader();
            bamRecordGuesser = getBamRecordGuesser(conf, partitionPath, header);
          }
        }
        for (int up = 0; up < block.uSize; up++) {
          index++;
          if (index > MAX_READ_SIZE) {
            return null;
          }
          long vPos = BgzfVirtualFilePointerUtil.makeFilePointer(block.pos, up);
          // As the guesser goes to the next BGZF block before looking for BAM
          // records, the ending BGZF blocks have to always be traversed fully.
          // Hence force the length to be 0xffff, the maximum possible.
          long vEnd = BgzfVirtualFilePointerUtil.makeFilePointer(block.end, 0xffff);
          if (bamRecordGuesser.checkRecordStart(vPos)) {
            block.end();
            return new PathChunk(partitionPath, new Chunk(vPos, vEnd));
          }
        }
      }
    } finally {
      if (bamRecordGuesser != null) {
        bamRecordGuesser.close();
      }
    }
    return pathChunk;
  }

  private BamRecordGuesser getBamRecordGuesser(
      Configuration conf, String path, SAMFileHeader header) throws IOException {
    SeekableStream ss = new ExtSeekableBufferedStream(fileSystemWrapper.open(conf, path));
    return new BamRecordGuesser(ss, header.getSequenceDictionary().size(), header);
  }

  /**
   * @return an RDD of reads for a bounded traversal (intervals and whether to return unplaced,
   *     unmapped reads).
   */
  @Override
  public <T extends Locatable> JavaRDD<SAMRecord> getReads(
      JavaSparkContext jsc,
      String path,
      int splitSize,
      HtsjdkReadsTraversalParameters<T> traversalParameters,
      ValidationStringency validationStringency,
      String referenceSourcePath)
      throws IOException {
    if (traversalParameters != null
        && traversalParameters.getIntervalsForTraversal() == null
        && !traversalParameters.getTraverseUnplacedUnmapped()) {
      throw new IllegalArgumentException("Traversing mapped reads only is not supported.");
    }

    Broadcast<HtsjdkReadsTraversalParameters<T>> traversalParametersBroadcast =
        traversalParameters == null ? null : jsc.broadcast(traversalParameters);
    SerializableHadoopConfiguration confSer =
        new SerializableHadoopConfiguration(jsc.hadoopConfiguration());

    return bgzfBlockSource
        .getBgzfBlocks(jsc, path, splitSize)
        .mapPartitions(
            (FlatMapFunction<Iterator<BgzfBlock>, SAMRecord>)
                bgzfBlocks -> {
                  Configuration c = confSer.getConf();
                  PathChunk pathChunk =
                      getFirstReadInPartition(
                          c, bgzfBlocks, validationStringency, referenceSourcePath);
                  if (pathChunk == null) {
                    return Collections.emptyIterator();
                  }
                  String p = pathChunk.getPath();
                  SamReader samReader =
                      createSamReader(c, p, validationStringency, referenceSourcePath);
                  BAMFileReader bamFileReader = createBamFileReader(samReader);
                  BAMFileSpan splitSpan = new BAMFileSpan(pathChunk.getSpan());
                  HtsjdkReadsTraversalParameters<T> traversal =
                      traversalParametersBroadcast == null
                          ? null
                          : traversalParametersBroadcast.getValue();
                  if (traversal == null) {
                    // no intervals or unplaced, unmapped reads
                    return new AutocloseIteratorWrapper<>(
                        bamFileReader.getIterator(splitSpan), samReader);
                  } else {
                    if (!samReader.hasIndex()) {
                      throw new IllegalArgumentException(
                          "Intervals set but no BAM index file found for " + p);
                    }
                    BAMIndex idx = samReader.indexing().getIndex();
                    Iterator<SAMRecord> intervalReadsIterator;
                    if (traversal.getIntervalsForTraversal() == null
                        || traversal.getIntervalsForTraversal().isEmpty()) {
                      intervalReadsIterator = Collections.emptyIterator();
                    } else {
                      SAMFileHeader header = samReader.getFileHeader();
                      QueryInterval[] queryIntervals =
                          BoundedTraversalUtil.prepareQueryIntervals(
                              traversal.getIntervalsForTraversal(), header.getSequenceDictionary());
                      BAMFileSpan span = BAMFileReader.getFileSpan(queryIntervals, idx);
                      span = (BAMFileSpan) span.removeContentsBefore(splitSpan);
                      span = (BAMFileSpan) span.removeContentsAfter(splitSpan);
                      intervalReadsIterator =
                          new AutocloseIteratorWrapper<>(
                              bamFileReader.createIndexIterator(
                                  queryIntervals, false, span.toCoordinateArray()),
                              samReader);
                    }

                    // add on unplaced unmapped reads if there are any in this range
                    if (traversal.getTraverseUnplacedUnmapped()) {
                      long startOfLastLinearBin = idx.getStartOfLastLinearBin();
                      long noCoordinateCount = ((AbstractBAMFileIndex) idx).getNoCoordinateCount();
                      if (startOfLastLinearBin != -1 && noCoordinateCount > 0) {
                        long unplacedUnmappedStart = startOfLastLinearBin;
                        if (pathChunk.getSpan().getChunkStart() <= unplacedUnmappedStart
                            && unplacedUnmappedStart
                                < pathChunk.getSpan().getChunkEnd()) { // TODO correct?
                          SamReader unplacedUnmappedReadsSamReader =
                              createSamReader(c, p, validationStringency, referenceSourcePath);
                          Iterator<SAMRecord> unplacedUnmappedReadsIterator =
                              new AutocloseIteratorWrapper<>(
                                  unplacedUnmappedReadsSamReader.queryUnmapped(),
                                  unplacedUnmappedReadsSamReader);
                          return Iterators.concat(
                              intervalReadsIterator, unplacedUnmappedReadsIterator);
                        }
                      }
                      if (traversal.getIntervalsForTraversal() == null) {
                        samReader.close(); // not used any more
                      }
                    }
                    return intervalReadsIterator;
                  }
                });
  }

  private BAMFileReader createBamFileReader(SamReader samReader) {
    BAMFileReader bamFileReader =
        (BAMFileReader) ((PrimitiveSamReaderToSamReaderAdapter) samReader).underlyingReader();
    if (bamFileReader.hasIndex()) {
      bamFileReader
          .getIndex(); // force BAMFileReader#mIndex to be populated so the index stream is properly
      // closed by the close() method
    }
    return bamFileReader;
  }
}
