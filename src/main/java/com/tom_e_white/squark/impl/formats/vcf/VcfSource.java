package com.tom_e_white.squark.impl.formats.vcf;

import com.tom_e_white.squark.impl.file.FileSystemWrapper;
import com.tom_e_white.squark.impl.file.HadoopFileSystemWrapper;
import com.tom_e_white.squark.impl.formats.bgzf.BGZFCodec;
import com.tom_e_white.squark.impl.formats.bgzf.BGZFEnhancedGzipCodec;
import com.tom_e_white.squark.impl.formats.tribble.TribbleIndexIntervalFilteringTextInputFormat;
import htsjdk.samtools.Defaults;
import htsjdk.samtools.SamStreams;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.Locatable;
import htsjdk.samtools.util.OverlapDetector;
import htsjdk.tribble.FeatureCodecHeader;
import htsjdk.tribble.TribbleException;
import htsjdk.tribble.index.Index;
import htsjdk.tribble.index.IndexFactory;
import htsjdk.tribble.readers.AsciiLineReader;
import htsjdk.tribble.readers.AsciiLineReaderIterator;
import htsjdk.tribble.util.TabixUtils;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

public class VcfSource implements Serializable {

  private FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();

  public VCFHeader getFileHeader(JavaSparkContext jsc, String path) throws IOException {
    Configuration conf = jsc.hadoopConfiguration();
    String firstVcfPath;
    if (fileSystemWrapper.isDirectory(conf, path)) {
      firstVcfPath = fileSystemWrapper.firstFileInDirectory(conf, path);
    } else {
      firstVcfPath = path;
    }
    try (SeekableStream headerIn =
        fileSystemWrapper.open(jsc.hadoopConfiguration(), firstVcfPath)) {
      BufferedInputStream bis = new BufferedInputStream(headerIn);
      // despite the name, isGzippedSAMFile looks for any gzipped stream
      InputStream is = SamStreams.isGzippedSAMFile(bis) ? new GZIPInputStream(bis) : bis;
      FeatureCodecHeader featureCodecHeader =
          new VCFCodec().readHeader(new AsciiLineReaderIterator(AsciiLineReader.from(is)));
      return (VCFHeader) featureCodecHeader.getHeaderValue();
    }
  }

  public <T extends Locatable> JavaRDD<VariantContext> getVariants(
      JavaSparkContext jsc, String path, int splitSize, List<T> intervals) throws IOException {

    // Use Hadoop FileSystem API to maintain file locality by using Hadoop's FileInputFormat

    final Configuration conf = jsc.hadoopConfiguration();
    if (splitSize > 0) {
      conf.setInt(FileInputFormat.SPLIT_MAXSIZE, splitSize);
    }
    enableBGZFCodecs(conf);

    VCFHeader vcfHeader = getFileHeader(jsc, path);
    Broadcast<VCFHeader> vcfHeaderBroadcast = jsc.broadcast(vcfHeader);
    Broadcast<List<T>> intervalsBroadcast = intervals == null ? null : jsc.broadcast(intervals);

    return textFile(jsc, conf, path, intervals)
        .mapPartitions(
            (FlatMapFunction<Iterator<String>, VariantContext>)
                lines -> {
                  VCFCodec codec = new VCFCodec(); // Use map partitions so we can reuse codec (not
                  // broadcast-able)
                  codec.setVCFHeader(
                      vcfHeaderBroadcast.getValue(),
                      VCFHeaderVersion.VCF4_1); // TODO: how to determine version?
                  final OverlapDetector<T> overlapDetector =
                      intervalsBroadcast == null
                          ? null
                          : OverlapDetector.create(intervalsBroadcast.getValue());
                  return stream(lines)
                      .filter(line -> !line.startsWith("#"))
                      .map(codec::decode)
                      .filter(vc -> overlapDetector == null || overlapDetector.overlapsAny(vc))
                      .iterator();
                });
  }

  private void enableBGZFCodecs(Configuration conf) {
    List<Class<? extends CompressionCodec>> codecs = CompressionCodecFactory.getCodecClasses(conf);
    if (!codecs.contains(BGZFEnhancedGzipCodec.class)) {
      codecs.remove(GzipCodec.class);
      codecs.add(BGZFEnhancedGzipCodec.class);
    }
    if (!codecs.contains(BGZFCodec.class)) {
      codecs.add(BGZFCodec.class);
    }
    CompressionCodecFactory.setCodecClasses(conf, new ArrayList<>(codecs));
  }

  private <T extends Locatable> JavaRDD<String> textFile(
      JavaSparkContext jsc, Configuration conf, String path, List<T> intervals) throws IOException {
    if (intervals == null) {
      // Use this over JavaSparkContext#textFile since this allows the configuration to be passed in
      return jsc.newAPIHadoopFile(
              path,
              TextInputFormat.class,
              LongWritable.class,
              Text.class,
              jsc.hadoopConfiguration())
          .map(pair -> pair._2.toString())
          .setName(path);
    } else {
      String indexPath;
      VcfFormat vcfFormat = VcfFormat.fromPath(path);
      if (vcfFormat == null) {
        indexPath = path + TabixUtils.STANDARD_INDEX_EXTENSION; // try tabix
      } else {
        indexPath = path + vcfFormat.getIndexExtension();
      }
      if (!fileSystemWrapper.exists(conf, indexPath)) {
        throw new IllegalArgumentException(
            "Intervals set but no index file found for " + path + " at " + indexPath);
      }
      try (InputStream indexIn =
          indexFileInputStream(indexPath, fileSystemWrapper.open(conf, indexPath))) {
        Index index = loadIndex(indexPath, indexIn);
        TribbleIndexIntervalFilteringTextInputFormat.setIndex(index);
        TribbleIndexIntervalFilteringTextInputFormat.setIntervals(intervals);
        return jsc.newAPIHadoopFile(
                path,
                TribbleIndexIntervalFilteringTextInputFormat.class,
                LongWritable.class,
                Text.class,
                jsc.hadoopConfiguration())
            .map(pair -> pair._2.toString())
            .setName(path);
      }
    }
  }

  // TODO: adapted from htsjdk - figure out how to share code
  public static Index loadIndex(String indexFile, InputStream in) {
    // Must be buffered, because getIndexType uses mark and reset
    try (BufferedInputStream bufferedInputStream =
        new BufferedInputStream(in, Defaults.NON_ZERO_BUFFER_SIZE)) {
      final Class<Index> indexClass =
          IndexFactory.IndexType.getIndexType(bufferedInputStream).getIndexType();
      final Constructor<Index> ctor = indexClass.getConstructor(InputStream.class);
      return ctor.newInstance(bufferedInputStream);
    } catch (final TribbleException ex) {
      throw ex;
    } catch (final IOException ex) {
      throw new TribbleException.UnableToReadIndexFile("Unable to read index file", indexFile, ex);
    } catch (final InvocationTargetException ex) {
      if (ex.getCause() instanceof EOFException) {
        throw new TribbleException.CorruptedIndexFile("Index file is corrupted", indexFile, ex);
      }
      throw new RuntimeException(ex);
    } catch (final Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private static InputStream indexFileInputStream(String indexPath, InputStream inputStreamInitial)
      throws IOException {
    if (indexPath.endsWith(".gz")) {
      return new GZIPInputStream(inputStreamInitial);
    } else if (indexPath.endsWith(TabixUtils.STANDARD_INDEX_EXTENSION)) {
      return new BlockCompressedInputStream(inputStreamInitial);
    } else {
      return inputStreamInitial;
    }
  }
  // end from htsjdk

  private static <T> Stream<T> stream(final Iterator<T> iterator) {
    return StreamSupport.stream(((Iterable<T>) () -> iterator).spliterator(), false);
  }
}
