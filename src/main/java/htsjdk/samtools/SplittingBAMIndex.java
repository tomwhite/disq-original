package htsjdk.samtools;

import com.tom_e_white.squark.impl.formats.bgzf.BgzfVirtualFilePointerUtil;
import htsjdk.samtools.util.BinaryCodec;
import htsjdk.samtools.util.BlockCompressedFilePointerUtil;
import htsjdk.samtools.util.RuntimeEOFException;
import htsjdk.samtools.util.RuntimeIOException;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * An index into BAM files, which records the file position of the start of every <i>n</i>th record.
 * Reads files that are created by {@link SplittingBAMIndexer}.
 *
 * <p>Indexes the positions of individual BAM records in the file.
 */
public final class SplittingBAMIndex {
  public static final String FILE_EXTENSION = ".sbi";

  /** Splitting BAM index file magic number. */
  static final byte[] SPLITTING_BAM_INDEX_MAGIC = "SBI\1".getBytes();

  private final int granularity;
  private final NavigableSet<Long> virtualOffsets = new TreeSet<>();

  /**
   * Create an in-memory splitting BAM index with the given virtual offsets.
   *
   * @param virtualOffsets the offsets in the index
   */
  public SplittingBAMIndex(final int granularity, final NavigableSet<Long> virtualOffsets) {
    this.granularity = granularity;
    this.virtualOffsets.addAll(virtualOffsets);
    if (virtualOffsets.isEmpty()) {
      throw new RuntimeIOException(
          "Invalid splitting BAM index: should contain at least the file size");
    }
  }

  /**
   * Load a splitting BAM index from a path.
   *
   * @param path the path to the splitting BAM index
   * @throws IOException as per java IO contract
   */
  public static SplittingBAMIndex load(final Path path) throws IOException {
    try (InputStream in = new BufferedInputStream(Files.newInputStream(path))) {
      return readIndex(in);
    }
  }

  /**
   * Load a splitting BAM index from a stream.
   *
   * @param in the stream to read the splitting BAM index from
   */
  public static SplittingBAMIndex load(final InputStream in) {
    return readIndex(in);
  }

  private static SplittingBAMIndex readIndex(final InputStream in) {
    BinaryCodec binaryCodec = new BinaryCodec(in);
    readHeader(binaryCodec);
    int granularity = binaryCodec.readInt();
    NavigableSet<Long> virtualOffsets = new TreeSet<>();
    try {
      long prev = -1;
      while (true) {
        long cur = binaryCodec.readLong();
        if (prev > cur) {
          throw new RuntimeIOException(
              String.format(
                  "Invalid splitting BAM index; offsets not in order: %#x > %#x", prev, cur));
        }
        virtualOffsets.add(prev = cur);
      }
    } catch (RuntimeEOFException e) {
      // signals end of index
    }
    return new SplittingBAMIndex(granularity, virtualOffsets);
  }

  private static void readHeader(BinaryCodec binaryCodec) {
    final byte[] buffer = new byte[SPLITTING_BAM_INDEX_MAGIC.length];
    binaryCodec.readBytes(buffer);
    if (!Arrays.equals(buffer, SPLITTING_BAM_INDEX_MAGIC)) {
      throw new RuntimeIOException(
          "Invalid file header in splitting BAM index: "
              + new String(buffer)
              + " ("
              + Arrays.toString(buffer)
              + ")");
    }
  }

  /**
   * Returns the granularity of the index, that is the number of alignments between subsequent
   * entries in the index.
   *
   * @return the granularity of the index
   */
  public int getGranularity() {
    return granularity;
  }

  /**
   * Returns the entries in the index.
   *
   * @return a set of file pointers for all the alignment offsets in the index, in ascending order.
   *     The last virtual file pointer is the (virtual) length of the file.
   */
  public NavigableSet<Long> getVirtualOffsets() {
    return new TreeSet<>(virtualOffsets);
  }

  /**
   * Returns number of entries in the index.
   *
   * @return the number of virtual offsets in the index
   */
  public int size() {
    return virtualOffsets.size();
  }

  /**
   * Returns size of the BAM file, in bytes.
   *
   * @return the length of the BAM file, in bytes
   */
  public long bamSize() {
    return BlockCompressedFilePointerUtil.getBlockAddress(virtualOffsets.last());
  }

  /**
   * Split the BAM file for this index into non-overlapping chunks of roughly the given size that
   * cover the whole file and that can be read independently of one another.
   *
   * @param splitSize the rough size of each split in bytes
   * @return a list of contiguous, non-overlapping, sorted chunks that cover the whole BAM file
   * @see #getChunk(long, long)
   */
  public List<Chunk> split(int splitSize) {
    if (splitSize <= 0) {
      throw new IllegalArgumentException(
          String.format("Split size must be positive: %s", splitSize));
    }
    long fileSize = bamSize();
    List<Chunk> chunks = new ArrayList<>();
    for (int splitStart = 0; splitStart < fileSize; splitStart += splitSize) {
      Chunk chunk = getChunk(splitStart, splitStart + splitSize);
      if (chunk != null) {
        chunks.add(chunk);
      }
    }
    return chunks;
  }

  /**
   * Return a chunk that corresponds to the given range in the BAM file. Note that the chunk does
   * not necessarily completely cover the given range, however this method will map a set of
   * contiguous, non-overlapping file ranges that cover the whole BAM file to a set of contiguous,
   * non-overlapping chunks that cover the whole file.
   *
   * @param splitStart the start of the file range (inclusive)
   * @param splitEnd the start of the file range (exclusive)
   * @return a chunk whose virtual start is at the first alignment start position that is greater
   *     than or equal to the given split start position, and whose virtual end is at the first
   *     alignment start position that is greater than or equal to the given split end position, or
   *     null if the chunk would be empty.
   * @see #split(int)
   */
  public Chunk getChunk(long splitStart, long splitEnd) {
    if (splitStart >= splitEnd) {
      throw new IllegalArgumentException(
          String.format("Split start (%s) must be less than end (%s)", splitStart, splitEnd));
    }
    long fileSize = bamSize();
    if (splitEnd > fileSize) {
      splitEnd = fileSize;
    }
    long virtualSplitStart = BgzfVirtualFilePointerUtil.makeFilePointer(splitStart);
    long virtualSplitEnd = BgzfVirtualFilePointerUtil.makeFilePointer(splitEnd);
    Long virtualSplitStartAlignment = virtualOffsets.ceiling(virtualSplitStart);
    Long virtualSplitEndAlignment = virtualOffsets.ceiling(virtualSplitEnd);
    // neither virtualSplitStartAlignment nor virtualSplitEndAlignment should ever be null, but
    // check anyway
    if (virtualSplitStartAlignment == null) {
      throw new IllegalArgumentException(
          String.format(
              "No virtual offset found for virtual file pointer %s, last virtual offset %s",
              virtualSplitStart, virtualOffsets.last()));
    }
    if (virtualSplitEndAlignment == null) {
      throw new IllegalArgumentException(
          String.format(
              "No virtual offset found for virtual file pointer %s, last virtual offset %s",
              virtualSplitEnd, virtualOffsets.last()));
    }
    if (virtualSplitStartAlignment.longValue() == virtualSplitEndAlignment.longValue()) {
      return null;
    }
    return new Chunk(virtualSplitStartAlignment, virtualSplitEndAlignment);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SplittingBAMIndex that = (SplittingBAMIndex) o;

    return virtualOffsets.equals(that.virtualOffsets);
  }

  @Override
  public int hashCode() {
    return virtualOffsets.hashCode();
  }

  @Override
  public String toString() {
    return virtualOffsets.toString();
  }
}
