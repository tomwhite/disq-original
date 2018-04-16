package com.tom_e_white.squark.impl.formats.sam;

import com.tom_e_white.squark.HtsjdkReadsTraversalParameters;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.util.Locatable;
import htsjdk.samtools.util.OverlapDetector;

// TODO: it would be nice if htsjdk's OverlapDetector was extensible
class TraversalOverlapDetector<T extends Locatable> {
  private final boolean traverseUnplacedUnmapped;
  private final OverlapDetector<T> overlapDetector;

  public TraversalOverlapDetector(HtsjdkReadsTraversalParameters<T> traversalParameters) {
    this.traverseUnplacedUnmapped = traversalParameters.getTraverseUnplacedUnmapped();
    if (traversalParameters.getIntervalsForTraversal() == null
        || traversalParameters.getIntervalsForTraversal().isEmpty()) {
      this.overlapDetector = null; // no intervals means 'no mapped reads'
    } else {
      this.overlapDetector = OverlapDetector.create(traversalParameters.getIntervalsForTraversal());
    }
  }

  public boolean overlapsAny(SAMRecord record) {
    if (traverseUnplacedUnmapped
        && record.getReadUnmappedFlag()
        && record.getAlignmentStart() == SAMRecord.NO_ALIGNMENT_START) {
      return true; // include record if unmapped records should be traversed and record is unmapped
    }
    return overlapDetector != null && overlapDetector.overlapsAny(record);
  }
}
