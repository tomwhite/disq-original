package com.tom_e_white.squark.impl.formats.cram;

import com.google.common.collect.Iterators;
import com.tom_e_white.squark.HtsjdkReadsTraversalParameters;
import com.tom_e_white.squark.impl.file.*;
import com.tom_e_white.squark.impl.formats.AutocloseIteratorWrapper;
import com.tom_e_white.squark.impl.formats.BoundedTraversalUtil;
import com.tom_e_white.squark.impl.formats.SerializableHadoopConfiguration;
import com.tom_e_white.squark.impl.formats.bgzf.BgzfVirtualFilePointerUtil;
import com.tom_e_white.squark.impl.formats.sam.AbstractSamSource;
import com.tom_e_white.squark.impl.formats.sam.SamFormat;
import htsjdk.samtools.*;
import htsjdk.samtools.SamReader.PrimitiveSamReaderToSamReaderAdapter;
import htsjdk.samtools.cram.CRAIEntry;
import htsjdk.samtools.cram.CRAIIndex;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.cram.structure.Container;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.Locatable;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

public class CramSource extends AbstractSamSource implements Serializable {

  private final PathSplitSource pathSplitSource;

  public CramSource(boolean useNio) {
    super(useNio ? new NioFileSystemWrapper() : new HadoopFileSystemWrapper());
    this.pathSplitSource = new PathSplitSource(useNio);
  }

  @Override
  public SamFormat getSamFormat() {
    return SamFormat.CRAM;
  }

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

    return getPathChunks(jsc, path, splitSize, validationStringency, referenceSourcePath)
        .mapPartitions(
            (FlatMapFunction<Iterator<PathChunk>, SAMRecord>)
                pathChunks -> {
                  Configuration c = confSer.getConf();
                  if (!pathChunks.hasNext()) {
                    return Collections.emptyIterator();
                  }
                  PathChunk pathChunk = pathChunks.next();
                  if (pathChunks.hasNext()) {
                    throw new IllegalArgumentException(
                        "Should not have more than one path chunk per partition");
                  }
                  String p = pathChunk.getPath();
                  SamReader samReader =
                      createSamReader(c, p, validationStringency, referenceSourcePath);
                  CRAMFileReader cramFileReader = createCramFileReader(samReader);
                  BAMFileSpan splitSpan = new BAMFileSpan(pathChunk.getSpan());
                  HtsjdkReadsTraversalParameters<T> traversal =
                      traversalParametersBroadcast == null
                          ? null
                          : traversalParametersBroadcast.getValue();
                  if (traversal == null) {
                    // no intervals or unplaced, unmapped reads
                    return new AutocloseIteratorWrapper<>(
                        cramFileReader.getIterator(splitSpan), samReader);
                  } else {
                    if (!samReader.hasIndex()) {
                      throw new IllegalArgumentException(
                          "Intervals set but no CRAM index file found for " + p);
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
                      SeekableStream ss = fileSystemWrapper.open(c, p);
                      // TODO: should go through FileSystemWrapper
                      ReferenceSource referenceSource =
                          new ReferenceSource(NioFileSystemWrapper.asPath(referenceSourcePath));
                      intervalReadsIterator =
                          new AutocloseIteratorWrapper<>(
                              new CRAMIntervalIterator(
                                  queryIntervals,
                                  false,
                                  idx,
                                  ss,
                                  referenceSource,
                                  validationStringency,
                                  span.toCoordinateArray()),
                              ss);
                    }

                    // add on unplaced unmapped reads if there are any in this range
                    if (traversal.getTraverseUnplacedUnmapped()) {
                      long startOfLastLinearBin = idx.getStartOfLastLinearBin();
                      // noCoordinateCount always seems to be 0, so ignore
                      if (startOfLastLinearBin != -1) {
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

  private JavaRDD<PathChunk> getPathChunks(
      JavaSparkContext jsc,
      String path,
      int splitSize,
      ValidationStringency stringency,
      String referenceSourcePath)
      throws IOException {

    final Configuration conf = jsc.hadoopConfiguration();

    // store paths (not full URIs) to avoid differences in scheme - this could be improved
    Map<String, List<Long>> pathToContainerOffsets = new LinkedHashMap<>();
    if (fileSystemWrapper.isDirectory(conf, path)) {
      List<String> paths =
          fileSystemWrapper
              .listDirectory(conf, path)
              .stream()
              .filter(SamFormat.CRAM::fileMatches)
              .collect(Collectors.toList());
      for (String p : paths) {
        long cramFileLength = fileSystemWrapper.getFileLength(conf, p);
        List<Long> containerOffsets = getContainerOffsetsFromIndex(conf, p, cramFileLength);
        String normPath = URI.create(fileSystemWrapper.normalize(conf, p)).getPath();
        pathToContainerOffsets.put(normPath, containerOffsets);
      }
    } else {
      long cramFileLength = fileSystemWrapper.getFileLength(conf, path);
      List<Long> containerOffsets = getContainerOffsetsFromIndex(conf, path, cramFileLength);
      String normPath = URI.create(fileSystemWrapper.normalize(conf, path)).getPath();
      pathToContainerOffsets.put(normPath, containerOffsets);
    }
    Broadcast<Map<String, List<Long>>> containerOffsetsBroadcast =
        jsc.broadcast(pathToContainerOffsets);

    SerializableHadoopConfiguration confSer =
        new SerializableHadoopConfiguration(jsc.hadoopConfiguration());

    return pathSplitSource
        .getPathSplits(jsc, path, splitSize)
        .flatMap(
            (FlatMapFunction<PathSplit, PathChunk>)
                pathSplit -> {
                  Configuration c = confSer.getConf();
                  String p = pathSplit.getPath();
                  Map<String, List<Long>> pathToOffsets = containerOffsetsBroadcast.getValue();
                  String normPath = URI.create(fileSystemWrapper.normalize(c, p)).getPath();
                  List<Long> offsets = pathToOffsets.get(normPath);
                  long newStart = nextContainerOffset(offsets, pathSplit.getStart());
                  long newEnd = nextContainerOffset(offsets, pathSplit.getEnd());
                  if (newStart == newEnd) {
                    return Collections.emptyIterator();
                  }
                  // TODO: test edge cases
                  // Subtract one from end since CRAMIterator's boundaries are inclusive
                  PathChunk pathChunk =
                      new PathChunk(
                          p,
                          new Chunk(
                              BgzfVirtualFilePointerUtil.makeFilePointer(newStart),
                              BgzfVirtualFilePointerUtil.makeFilePointer(newEnd - 1)));
                  return Collections.singleton(pathChunk).iterator();
                });
  }

  private List<Long> getContainerOffsetsFromIndex(
      Configuration conf, String path, long cramFileLength) throws IOException {
    try (SeekableStream in = findIndex(conf, path)) {
      if (in == null) {
        return getContainerOffsetsFromFile(conf, path, cramFileLength);
      }
      List<Long> containerOffsets = new ArrayList<>();
      CRAIIndex index = CRAMCRAIIndexer.readIndex(in);
      for (CRAIEntry entry : index.getCRAIEntries()) {
        containerOffsets.add(entry.containerStartOffset);
      }
      containerOffsets.add(cramFileLength);
      return containerOffsets;
    }
  }

  private List<Long> getContainerOffsetsFromFile(
      Configuration conf, String path, long cramFileLength) throws IOException {
    try (SeekableStream seekableStream = fileSystemWrapper.open(conf, path)) {
      CramContainerHeaderIterator it = new CramContainerHeaderIterator(seekableStream);
      List<Long> containerOffsets = new ArrayList<Long>();
      while (it.hasNext()) {
        Container container = it.next();
        containerOffsets.add(container.offset);
      }
      containerOffsets.add(cramFileLength);
      return containerOffsets;
    }
  }

  private static long nextContainerOffset(List<Long> containerOffsets, long position) {
    int index = Collections.binarySearch(containerOffsets, position);
    long offset;
    if (index >= 0) {
      offset = containerOffsets.get(index);
    } else {
      int insertionPoint = -index - 1;
      if (insertionPoint == containerOffsets.size()) {
        throw new IllegalStateException(
            "Could not find position "
                + position
                + " in "
                + "container offsets: "
                + containerOffsets);
      }
      offset = containerOffsets.get(insertionPoint);
    }
    return offset;
  }

  private CRAMFileReader createCramFileReader(SamReader samReader) throws IOException {
    return (CRAMFileReader) ((PrimitiveSamReaderToSamReaderAdapter) samReader).underlyingReader();
  }
}
