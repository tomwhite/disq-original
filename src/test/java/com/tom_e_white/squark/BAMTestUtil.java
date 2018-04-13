package com.tom_e_white.squark;

import htsjdk.samtools.BAMIndex;
import htsjdk.samtools.BAMIndexer;
import htsjdk.samtools.CRAMCRAIIndexer;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileWriter;
import htsjdk.samtools.SAMFileWriterFactory;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordSetBuilder;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.seekablestream.SeekableFileStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class BAMTestUtil {
  public static File writeBamFile(int numPairs, SAMFileHeader.SortOrder sortOrder)
      throws IOException {
    return writeBamFile(numPairs, sortOrder, ".bam", BAMIndex.BAMIndexSuffix, null);
  }

  public static File writeBamFile(
      int numPairs,
      SAMFileHeader.SortOrder sortOrder,
      String extension,
      String indexExtension,
      File refFile)
      throws IOException {
    // file will be both queryname and coordinate sorted, so use one or the other
    SAMRecordSetBuilder samRecordSetBuilder = new SAMRecordSetBuilder(true, sortOrder);
    for (int i = 0; i < numPairs; i++) {
      int chr = 20;
      int start1 = (i + 1) * 1000;
      int start2 = start1 + 100;
      if (i == 5) { // add two unmapped fragments instead of a mapped pair
        samRecordSetBuilder.addFrag(
            String.format("test-read-%03d-1", i), chr, start1, false, true, null, null, -1, false);
        samRecordSetBuilder.addFrag(
            String.format("test-read-%03d-2", i), chr, start2, false, true, null, null, -1, false);
      } else {
        samRecordSetBuilder.addPair(String.format("test-read-%03d", i), chr, start1, start2);
      }
    }
    if (numPairs > 0) { // add two unplaced unmapped fragments if non-empty
      samRecordSetBuilder.addUnmappedFragment(
          String.format("test-read-%03d-unplaced-unmapped", numPairs++));
      samRecordSetBuilder.addUnmappedFragment(
          String.format("test-read-%03d-unplaced-unmapped", numPairs++));
    }

    final File bamFile = File.createTempFile("test", extension);
    bamFile.deleteOnExit();
    SAMFileHeader samHeader = samRecordSetBuilder.getHeader();
    final SAMFileWriter bamWriter;
    if (extension.equals(CramIO.CRAM_FILE_EXTENSION)) {
      bamWriter = new SAMFileWriterFactory().makeCRAMWriter(samHeader, true, bamFile, refFile);
    } else {
      bamWriter = new SAMFileWriterFactory().makeSAMOrBAMWriter(samHeader, true, bamFile);
    }
    for (final SAMRecord rec : samRecordSetBuilder.getRecords()) {
      bamWriter.addAlignment(rec);
    }
    bamWriter.close();

    // create BAM index
    if (sortOrder.equals(SAMFileHeader.SortOrder.coordinate)) {
      if (extension.equals(CramIO.CRAM_FILE_EXTENSION)) {
        OutputStream out =
            new FileOutputStream(new File(bamFile.getAbsolutePath() + indexExtension));
        CRAMCRAIIndexer.writeIndex(new SeekableFileStream(bamFile), out);
      } else {
        SamReader samReader =
            SamReaderFactory.makeDefault()
                .enable(SamReaderFactory.Option.INCLUDE_SOURCE_IN_RECORDS)
                .open(bamFile);
        BAMIndexer.createIndex(
            samReader,
            new File(bamFile.getAbsolutePath().replaceFirst(extension + "$", indexExtension)));
      }
    }

    return bamFile;
  }
}
