package com.tom_e_white.squark.impl.formats.sam;

import com.tom_e_white.squark.HtsjdkReadsTraversalParameters;
import com.tom_e_white.squark.impl.file.FileSystemWrapper;
import com.tom_e_white.squark.impl.file.NioFileSystemWrapper;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.Locatable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class AbstractSamSource implements Serializable {

  protected final FileSystemWrapper fileSystemWrapper;

  protected AbstractSamSource(FileSystemWrapper fileSystemWrapper) {
    this.fileSystemWrapper = fileSystemWrapper;
  }

  public abstract SamFormat getSamFormat();

  public SAMFileHeader getFileHeader(
      JavaSparkContext jsc,
      String path,
      ValidationStringency stringency,
      String referenceSourcePath)
      throws IOException {
    // TODO: support multiple headers
    Configuration conf = jsc.hadoopConfiguration();
    String firstSamPath;
    if (fileSystemWrapper.isDirectory(conf, path)) {
      firstSamPath = fileSystemWrapper.firstFileInDirectory(conf, path);
    } else {
      firstSamPath = path;
    }
    try (SamReader samReader =
        createSamReader(conf, firstSamPath, stringency, referenceSourcePath)) {
      return samReader.getFileHeader();
    }
  }

  public abstract <T extends Locatable> JavaRDD<SAMRecord> getReads(
      JavaSparkContext jsc,
      String path,
      int splitSize,
      HtsjdkReadsTraversalParameters<T> traversalParameters,
      ValidationStringency validationStringency,
      String referenceSourcePath)
      throws IOException;

  protected SamReader createSamReader(
      Configuration conf, String path, ValidationStringency stringency, String referenceSourcePath)
      throws IOException {
    SeekableStream in = fileSystemWrapper.open(conf, path);
    SeekableStream indexStream = findIndex(conf, path);
    SamReaderFactory readerFactory =
        SamReaderFactory.makeDefault()
            .setOption(SamReaderFactory.Option.CACHE_FILE_BASED_INDEXES, true)
            .setOption(SamReaderFactory.Option.EAGERLY_DECODE, false)
            .setUseAsyncIo(false);
    if (stringency != null) {
      readerFactory.validationStringency(stringency);
    }
    if (referenceSourcePath != null) {
      // TODO: should go through FileSystemWrapper, needs
      // https://github.com/samtools/htsjdk/pull/1123
      readerFactory.referenceSource(
          new ReferenceSource(NioFileSystemWrapper.asPath(referenceSourcePath)));
    }
    SamInputResource resource = SamInputResource.of(in);
    if (indexStream != null) {
      resource.index(indexStream);
    }
    return readerFactory.open(resource);
  }

  protected SeekableStream findIndex(Configuration conf, String path) throws IOException {
    SamFormat samFormat = getSamFormat();
    if (samFormat.getIndexExtension() == null) {
      return null; // doesn't support indexes
    }
    String index = path + samFormat.getIndexExtension();
    if (fileSystemWrapper.exists(conf, index)) {
      return fileSystemWrapper.open(conf, index);
    }
    index =
        path.replaceFirst(
            Pattern.quote(samFormat.getExtension()) + "$", samFormat.getIndexExtension());
    if (fileSystemWrapper.exists(conf, index)) {
      return fileSystemWrapper.open(conf, index);
    }
    return null;
  }

  protected static <T> Stream<T> stream(final Iterator<T> iterator) {
    return StreamSupport.stream(((Iterable<T>) () -> iterator).spliterator(), false);
  }
}
