package com.tom_e_white.squark.impl.formats.bgzf;

import com.tom_e_white.squark.impl.file.FileSplitInputFormat;
import com.tom_e_white.squark.impl.file.FileSystemWrapper;
import com.tom_e_white.squark.impl.file.HadoopFileSystemWrapper;
import com.tom_e_white.squark.impl.file.NioFileSystemWrapper;
import com.tom_e_white.squark.impl.formats.SerializableHadoopConfiguration;
import com.tom_e_white.squark.impl.formats.bgzf.BgzfBlockGuesser.BgzfBlock;
import htsjdk.samtools.util.AbstractIterator;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

/**
 * This class can find BGZF block boundaries in a distributed manner, and then iterate over all the
 * blocks efficiently. It is not meant to be used directly by users, but instead is used by other
 * libraries that are based on BGZF, such as {@link BamSource}.
 *
 * @see BamSource
 */
public class BgzfBlockSource implements Serializable {

  private final boolean useNio;
  private final FileSystemWrapper fileSystemWrapper;

  public BgzfBlockSource() {
    this(false);
  }

  /**
   * @param useNio if true use the NIO filesystem APIs rather than the Hadoop filesystem APIs. This
   *     is appropriate for cloud stores where file locality is not relied upon.
   */
  public BgzfBlockSource(boolean useNio) {
    this.useNio = useNio;
    this.fileSystemWrapper = useNio ? new NioFileSystemWrapper() : new HadoopFileSystemWrapper();
  }

  public JavaRDD<BgzfBlock> getBgzfBlocks(JavaSparkContext jsc, String path, int splitSize)
      throws IOException {
    if (useNio) {
      // Use Java NIO by creating splits with Spark parallelize. File locality is not maintained,
      // but this is not an issue if reading from a cloud store.

      // TODO: support case where path is a directory
      long len = fileSystemWrapper.getFileLength(null, path);
      int numSplits = (int) Math.ceil((double) len / splitSize);

      List<Long> range = LongStream.range(0, numSplits).boxed().collect(Collectors.toList());
      return jsc.parallelize(range, numSplits)
          .map(idx -> idx * splitSize)
          .flatMap(
              splitStart -> {
                final long splitEnd = splitStart + splitSize > len ? len : splitStart + splitSize;
                BgzfBlockGuesser bgzfBlockGuesser = getBgzfSplitGuesser(null, path);
                return getBgzfBlockIterator(bgzfBlockGuesser, splitStart, splitEnd);
              });
    } else {
      // Use Hadoop FileSystem API to maintain file locality by using Hadoop's FileInputFormat

      final Configuration conf = jsc.hadoopConfiguration();
      if (splitSize > 0) {
        conf.setInt(FileInputFormat.SPLIT_MAXSIZE, splitSize);
      }
      SerializableHadoopConfiguration confSer = new SerializableHadoopConfiguration(conf);
      return jsc.newAPIHadoopFile(
              path, FileSplitInputFormat.class, Void.class, FileSplit.class, conf)
          .flatMap(
              (FlatMapFunction<Tuple2<Void, FileSplit>, BgzfBlock>)
                  t2 -> {
                    FileSplit fileSplit = t2._2();
                    BgzfBlockGuesser bgzfBlockGuesser =
                        getBgzfSplitGuesser(confSer.getConf(), fileSplit.getPath().toString());
                    return getBgzfBlockIterator(bgzfBlockGuesser, fileSplit);
                  });
    }
    // TODO: drop final empty block
  }

  private BgzfBlockGuesser getBgzfSplitGuesser(Configuration conf, String path) throws IOException {
    return new BgzfBlockGuesser(fileSystemWrapper.open(conf, path), path);
  }

  /**
   * @return an iterator over all the {@link BgzfBlock}s that start in the given {@link FileSplit}.
   */
  private static Iterator<BgzfBlock> getBgzfBlockIterator(
      BgzfBlockGuesser bgzfBlockGuesser, FileSplit split) {
    return getBgzfBlockIterator(
        bgzfBlockGuesser, split.getStart(), split.getStart() + split.getLength());
  }

  private static Iterator<BgzfBlock> getBgzfBlockIterator(
      BgzfBlockGuesser bgzfBlockGuesser, long splitStart, long splitEnd) {
    return new AbstractIterator<BgzfBlock>() {
      long start = splitStart;

      @Override
      protected BgzfBlock advance() {
        if (start > splitEnd) {
          bgzfBlockGuesser.close();
          return null; // end iteration
        }
        BgzfBlock bgzfBlock = bgzfBlockGuesser.guessNextBGZFPos(start, splitEnd);
        if (bgzfBlock == null) {
          bgzfBlockGuesser.close();
          return null; // end iteration
        } else {
          start = bgzfBlock.pos + bgzfBlock.cSize;
        }
        return bgzfBlock;
      }
    };
  }
}
