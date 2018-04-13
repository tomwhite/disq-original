package htsjdk.samtools;

import htsjdk.samtools.cram.ref.CRAMReferenceSource;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.CloseableIterator;
import htsjdk.samtools.util.RuntimeEOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.NoSuchElementException;

public class CRAMIntervalIterator extends BAMQueryMultipleIntervalsIteratorFilter
    implements CloseableIterator<SAMRecord> {

  // the granularity of this iterator is the container, so the records returned
  // by it must still be filtered to find those matching the filter criteria
  private CRAMIterator unfilteredIterator;
  SAMRecord nextRec = null;

  public CRAMIntervalIterator(
      final QueryInterval[] queries,
      final boolean contained,
      BAMIndex index,
      SeekableStream ss,
      CRAMReferenceSource referenceSource,
      ValidationStringency validationStringency,
      long[] coordinates) {
    super(queries, contained);

    if (coordinates != null && coordinates.length != 0) {
      try {
        unfilteredIterator =
            new CRAMIterator(ss, referenceSource, coordinates, validationStringency);
      } catch (final IOException e) {
        throw new RuntimeEOFException(e);
      }
      getNextRecord(); // advance to the first record that matches the filter criteria
    }
  }

  // convert queries -> merged BAMFileSpan -> coordinate array
  private long[] coordinatesFromQueryIntervals(BAMIndex index, QueryInterval[] queries) {
    ArrayList<BAMFileSpan> spanList = new ArrayList<>(1);
    Arrays.asList(queries)
        .forEach(qi -> spanList.add(index.getSpanOverlapping(qi.referenceIndex, qi.start, qi.end)));
    BAMFileSpan spanArray[] = new BAMFileSpan[spanList.size()];
    for (int i = 0; i < spanList.size(); i++) {
      spanArray[i] = spanList.get(i);
    }

    return BAMFileSpan.merge(spanArray).toCoordinateArray();
  }

  @Override
  public void close() {
    if (unfilteredIterator != null) {
      unfilteredIterator.close();
    }
  }

  @Override
  public boolean hasNext() {
    return nextRec != null;
  }

  @Override
  public SAMRecord next() {
    if (!hasNext()) {
      throw new NoSuchElementException("Next called on empty CRAMIntervalIterator");
    }
    return getNextRecord();
  }

  private SAMRecord getNextRecord() {
    final SAMRecord result = nextRec;
    nextRec = null;
    while (nextRec == null && unfilteredIterator.hasNext()) {
      SAMRecord nextRecord = unfilteredIterator.next();
      switch (compareToFilter(nextRecord)) {
        case MATCHES_FILTER:
          nextRec = nextRecord;
          break;
        case CONTINUE_ITERATION:
          continue;
        case STOP_ITERATION:
          break;
        default:
          throw new SAMException("Unexpected return from compareToFilter");
      }
    }
    return result;
  }

  @Override
  public void remove() {
    throw new RuntimeException("Method \"remove\" not implemented for CRAMIntervalIterator.");
  }
}
