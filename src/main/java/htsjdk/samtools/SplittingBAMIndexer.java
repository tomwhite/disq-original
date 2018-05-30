package htsjdk.samtools;

import com.tom_e_white.squark.impl.formats.bgzf.BgzfVirtualFilePointerUtil;
import htsjdk.samtools.cram.io.InputStreamUtils;
import htsjdk.samtools.seekablestream.SeekablePathStream;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.*;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * An indexing tool and API for BAM files, making it possible to read BAM files in parallel by
 * finding a record after an arbitrary file split point. Writes splitting BAM indices as understood
 * by {@link SplittingBAMIndex}.
 *
 * <p>There are two ways of using this class: 1) Building a splitting BAM index from an existing BAM
 * file 2) Building a splitting BAM index while building the BAM file
 *
 * <p>For 1), use one of the static {@link #createIndex} methods, which take an input BAM file to be
 * indexed.
 *
 * <p>For 2), use one of the constructors that takes an output stream, then pass {@link SAMRecord}
 * objects via the {@link #processAlignment} method, and then call {@link #finish(long)} to complete
 * writing the index.
 */
public final class SplittingBAMIndexer {

  // Default to a granularity level of 4096. This is generally sufficient
  // for very large BAM files, relative to a maximum heap size in the
  // gigabyte range.
  public static final int DEFAULT_GRANULARITY = 4096;

  private final BinaryCodec binaryCodec;
  private final int granularity;
  private long count;

  /**
   * Prepare to index a BAM file.
   *
   * @param out the stream to write the index to
   */
  public SplittingBAMIndexer(final OutputStream out) {
    this(out, SplittingBAMIndexer.DEFAULT_GRANULARITY);
  }

  /**
   * Prepare to index a BAM file.
   *
   * @param out the stream to write the index to
   * @param granularity write the offset of every n-th alignment to the index
   */
  public SplittingBAMIndexer(final OutputStream out, final int granularity) {
    this.binaryCodec = new BinaryCodec(out);
    this.granularity = granularity;
    writeHeader();
    writeGranularity(granularity);
  }

  private void writeHeader() {
    binaryCodec.writeBytes(SplittingBAMIndex.SPLITTING_BAM_INDEX_MAGIC);
  }

  private void writeGranularity(int granularity) {
    binaryCodec.writeInt(granularity);
  }

  public static void createIndex(final Path bamFile, final int granularity) throws IOException {
    Path splittingBaiFile = IOUtil.addExtension(bamFile, SplittingBAMIndex.FILE_EXTENSION);
    try (SeekableStream in = new SeekablePathStream(bamFile);
        OutputStream out = Files.newOutputStream(splittingBaiFile)) {
      createIndex(in, out, granularity);
    }
  }

  /**
   * Perform indexing on the given BAM file, at the granularity level specified.
   *
   * @param in a seekable stream for reading the BAM file from
   * @param out the stream to write the index to
   * @param granularity write the offset of every n-th alignment to the index
   * @throws IOException as per java IO contract
   */
  public static void createIndex(
      final SeekableStream in, final OutputStream out, final int granularity) throws IOException {
    long firstRecordStart = findVirtualOffsetOfFirstRecordInBam(in);
    try (BlockCompressedInputStream blockIn = new BlockCompressedInputStream(in)) {
      blockIn.seek(firstRecordStart);
      final ByteBuffer byteBuffer =
          ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN); // BAM is little-endian
      SplittingBAMIndexer splittingBAMIndexer = new SplittingBAMIndexer(out, granularity);
      while (true) {
        try {
          final long recordStart = blockIn.getFilePointer();
          InputStreamUtils.readFully(blockIn, byteBuffer.array(), 0, 4);
          final int blockSize = byteBuffer.getInt(0); // length of remainder of alignment record
          splittingBAMIndexer.processAlignment(recordStart);
          skipFully(blockIn, blockSize);
        } catch (EOFException e) {
          break;
        }
      }
      splittingBAMIndexer.finish(in.length());
    }
  }

  /**
   * Process the given record for the index.
   *
   * @param rec the record from the file being indexed
   */
  public void processAlignment(final SAMRecord rec) {
    if (count++ % granularity == 0) {
      SAMFileSource source = rec.getFileSource();
      if (source == null) {
        throw new SAMException(
            "No source (virtual file offsets); needed for indexing on BAM Record " + rec);
      }
      BAMFileSpan filePointer = (BAMFileSpan) source.getFilePointer();
      writeVirtualOffset(filePointer.getFirstOffset());
    }
  }

  private void processAlignment(final long virtualOffset) {
    if (count++ % granularity == 0) {
      writeVirtualOffset(virtualOffset);
    }
  }

  /**
   * Write the given virtual offset to the index.
   *
   * @param virtualOffset virtual file pointer
   */
  public void writeVirtualOffset(long virtualOffset) {
    binaryCodec.writeLong(virtualOffset);
  }

  /**
   * Complete the index by writing the input BAM file size to the index, and closing the output
   * stream.
   *
   * @param inputSize the size of the input BAM file
   */
  public void finish(long inputSize) {
    writeVirtualOffset(BgzfVirtualFilePointerUtil.makeFilePointer(inputSize));
    binaryCodec.close();
  }

  /**
   * Similar to readFully(). Skips bytes in a loop.
   *
   * @param in The InputStream to skip bytes from
   * @param length number of bytes to skip.
   * @throws IOException if it could not skip requested number of bytes for any reason (including
   *     EOF)
   */
  public static void skipFully(InputStream in, long length) throws IOException {
    long amt = length;
    while (amt > 0) {
      long ret = in.skip(amt);
      if (ret == 0) {
        // skip may return 0 even if we're not at EOF.  Luckily, we can
        // use the read() method to figure out if we're at the end.
        int b = in.read();
        if (b == -1) {
          throw new EOFException(
              "Premature EOF from inputStream after " + "skipping " + (length - amt) + " byte(s).");
        }
        ret = 1;
      }
      amt -= ret;
    }
  }

  /**
   * Returns the virtual file offset of the first record in a BAM file - i.e. the virtual file
   * offset after skipping over the text header and the sequence records.
   */
  public static long findVirtualOffsetOfFirstRecordInBam(final SeekableStream seekableStream) {
    try {
      return BAMFileReader2.findVirtualOffsetOfFirstRecord(seekableStream);
    } catch (final IOException ioe) {
      throw new RuntimeEOFException(ioe);
    }
  }
}
