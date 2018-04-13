package com.tom_e_white.squark;

import com.tom_e_white.squark.impl.formats.BoundedTraversalUtil;
import htsjdk.samtools.QueryInterval;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.util.Interval;
import htsjdk.samtools.util.Locatable;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class HtsjdkReadsRddTest extends BaseTest {

  private Object[] parametersForTestReadAndWrite() {
    return new Object[][] {
      {"1.bam", null, ".bam", 128 * 1024, false},
      {"1.bam", null, ".bam", 128 * 1024, true},
      {"valid.cram", "valid.fasta", ".cram", 128 * 1024, false},
      {"valid_no_index.cram", "valid.fasta", ".cram", 128 * 1024, false},
    };
  }

  @Test
  @Parameters
  public void testReadAndWrite(
      String inputFile,
      String cramReferenceFile,
      String outputExtension,
      int splitSize,
      boolean useNio)
      throws IOException, URISyntaxException {
    String inputPath = ClassLoader.getSystemClassLoader().getResource(inputFile).toURI().toString();

    HtsjdkReadsRddStorage htsjdkReadsRddStorage =
        HtsjdkReadsRddStorage.makeDefault(jsc).splitSize(splitSize).useNio(useNio);
    ReferenceSource referenceSource = null;
    if (cramReferenceFile != null) {
      String cramReferencePath =
          ClassLoader.getSystemClassLoader().getResource(cramReferenceFile).toURI().toString();
      htsjdkReadsRddStorage.referenceSourcePath(cramReferencePath);
      referenceSource =
          new ReferenceSource(
              new File(ClassLoader.getSystemClassLoader().getResource(cramReferenceFile).toURI()));
    }

    HtsjdkReadsRdd htsjdkReadsRdd = htsjdkReadsRddStorage.read(inputPath);

    int expectedCount =
        getBAMRecordCount(new File(inputPath.replace("file:", "")), referenceSource);
    Assert.assertEquals(expectedCount, htsjdkReadsRdd.getReads().count());

    File outputFile = File.createTempFile("test", outputExtension);
    outputFile.delete();
    String outputPath = outputFile.toURI().toString();

    htsjdkReadsRddStorage.write(htsjdkReadsRdd, outputPath);

    Assert.assertEquals(expectedCount, getBAMRecordCount(outputFile, referenceSource));
  }

  @Test
  public void testReadBamsInDirectory() throws IOException, URISyntaxException {
    HtsjdkReadsRddStorage htsjdkReadsRddStorage =
        HtsjdkReadsRddStorage.makeDefault(jsc).splitSize(128 * 1024);

    // directory containing two BAM files
    File inputDir =
        new File(
            ClassLoader.getSystemClassLoader()
                .getResource("HiSeq.1mb.1RG.2k_lines.alternate.recalibrated.DIQ.sharded.bam")
                .toURI());
    String inputPath = inputDir.toURI().toString();

    HtsjdkReadsRdd htsjdkReadsRdd = htsjdkReadsRddStorage.read(inputPath);

    int expectedCount =
        getBAMRecordCount(new File(inputDir, "part-r-00000.bam"))
            + getBAMRecordCount(new File(inputDir, "part-r-00001.bam"));
    Assert.assertEquals(expectedCount, htsjdkReadsRdd.getReads().count());
  }

  private Object[] parametersForTestIntervals() {
    return new Object[][] {
      {
        null,
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 5000, 9999), // includes two unpaired fragments
                new Interval("chr21", 20000, 22999)),
            false),
        ".bam",
        ".bai"
      },
      {
        null,
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 1, 1000135) // covers whole chromosome
                ),
            false),
        ".bam",
        ".bai"
      },
      {
        null,
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 5000, 9999), // includes two unpaired fragments
                new Interval("chr21", 20000, 22999)),
            true),
        ".bam",
        ".bai"
      },
      {null, new HtsjdkReadsTraversalParameters<>(null, true), ".bam", ".bai"},
      {
        "test.fa",
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 5000, 9999), // includes two unpaired fragments
                new Interval("chr21", 20000, 22999)),
            false),
        ".cram",
        ".crai"
      },
      {
        "test.fa",
        new HtsjdkReadsTraversalParameters<>(
            Arrays.asList(
                new Interval("chr21", 1, 1000135) // covers whole chromosome
                ),
            false),
        ".cram",
        ".crai"
      },
      //        {
      //            "test.fa",
      //            new HtsjdkReadsTraversalParameters<>(Arrays.asList(
      //                new Interval("chr21", 5000, 9999), // includes two unpaired fragments
      //                new Interval("chr21", 20000, 22999)
      //            ), true),
      //            ".cram", ".crai"
      //        },
      //        {
      //            "test.fa",
      //            new HtsjdkReadsTraversalParameters<>(null, true),
      //            ".cram", ".crai"
      //        },
    };
  }

  @Test
  @Parameters
  public <T extends Locatable> void testIntervals(
      String cramReferenceFile,
      HtsjdkReadsTraversalParameters<T> traversalParameters,
      String extension,
      String indexExtension)
      throws Exception {
    HtsjdkReadsRddStorage htsjdkReadsRddStorage =
        HtsjdkReadsRddStorage.makeDefault(jsc).splitSize(40000).useNio(false);

    ReferenceSource referenceSource = null;
    File refFile = null;
    if (cramReferenceFile != null) {
      refFile = new File(ClassLoader.getSystemClassLoader().getResource(cramReferenceFile).toURI());
      String cramReferencePath =
          ClassLoader.getSystemClassLoader().getResource(cramReferenceFile).toURI().toString();
      htsjdkReadsRddStorage.referenceSourcePath(cramReferencePath);
      referenceSource =
          new ReferenceSource(
              new File(ClassLoader.getSystemClassLoader().getResource(cramReferenceFile).toURI()));
    }

    String inputPath =
        BAMTestUtil.writeBamFile(
                1000, SAMFileHeader.SortOrder.coordinate, extension, indexExtension, refFile)
            .toURI()
            .toString();

    HtsjdkReadsRdd htsjdkReadsRdd = htsjdkReadsRddStorage.read(inputPath, traversalParameters);

    int expectedCount =
        getBAMRecordCount(
            new File(inputPath.replace("file:", "")), referenceSource, traversalParameters);

    Assert.assertEquals(expectedCount, htsjdkReadsRdd.getReads().count());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMappedOnlyFails() throws Exception {
    String inputPath =
        BAMTestUtil.writeBamFile(1000, SAMFileHeader.SortOrder.coordinate).toURI().toString();

    HtsjdkReadsRddStorage htsjdkReadsRddStorage =
        HtsjdkReadsRddStorage.makeDefault(jsc).splitSize(40000).useNio(false);

    htsjdkReadsRddStorage.read(inputPath, new HtsjdkReadsTraversalParameters<>(null, false));
  }

  private static int getBAMRecordCount(final File bamFile) throws IOException {
    return getBAMRecordCount(bamFile, null);
  }

  private static int getBAMRecordCount(final File bamFile, ReferenceSource referenceSource)
      throws IOException {
    return getBAMRecordCount(bamFile, referenceSource, null);
  }

  private static <T extends Locatable> int getBAMRecordCount(
      final File bamFile,
      ReferenceSource referenceSource,
      HtsjdkReadsTraversalParameters<T> traversalParameters)
      throws IOException {
    int recCount = 0;
    try (SamReader bamReader =
        SamReaderFactory.makeDefault()
            .referenceSource(referenceSource)
            .open(SamInputResource.of(bamFile))) {
      Iterator<SAMRecord> it;
      if (traversalParameters == null) {
        it = bamReader.iterator();
      } else if (traversalParameters.getIntervalsForTraversal() == null) {
        it = Collections.emptyIterator();
      } else {
        SAMSequenceDictionary sequenceDictionary =
            bamReader.getFileHeader().getSequenceDictionary();
        QueryInterval[] queryIntervals =
            BoundedTraversalUtil.prepareQueryIntervals(
                traversalParameters.getIntervalsForTraversal(), sequenceDictionary);
        it = bamReader.queryOverlapping(queryIntervals);
      }
      while (it.hasNext()) {
        it.next();
        recCount++;
      }
    }

    if (traversalParameters != null && traversalParameters.getTraverseUnplacedUnmapped()) {
      try (SamReader bamReader =
          SamReaderFactory.makeDefault()
              .referenceSource(referenceSource)
              .open(SamInputResource.of(bamFile))) {
        Iterator<SAMRecord> it = bamReader.queryUnmapped();
        while (it.hasNext()) {
          it.next();
          recCount++;
        }
      }
    }

    return recCount;
  }
}
