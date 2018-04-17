package com.tom_e_white.squark;

import com.google.common.io.Files;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.Interval;
import htsjdk.tribble.util.TabixUtils;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Iterator;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class HtsjdkVariantsRddTest extends BaseTest {

  private Object[] parametersForTestReadAndWrite() {
    return new Object[][] {
      {"test.vcf", ".vcf", 128 * 1024},
      {"test.vcf", ".vcf.gz", 128 * 1024},
      {"test.vcf", ".vcf.bgz", 128 * 1024},
      {"test.vcf.bgz", ".vcf", 128 * 1024},
      {"test.vcf.bgzf.gz", ".vcf", 128 * 1024},
      {"test.vcf.gz", ".vcf", 128 * 1024},
    };
  }

  @Test
  @Parameters
  public void testReadAndWrite(String inputFile, String outputExtension, int splitSize)
      throws IOException, URISyntaxException {
    String inputPath = ClassLoader.getSystemClassLoader().getResource(inputFile).toURI().toString();

    HtsjdkVariantsRddStorage htsjdkVariantsRddStorage =
        HtsjdkVariantsRddStorage.makeDefault(jsc).splitSize(splitSize);

    HtsjdkVariantsRdd htsjdkVariantsRdd = htsjdkVariantsRddStorage.read(inputPath);

    int expectedCount = getVariantCount(new File(inputPath.replace("file:", "")), null);
    Assert.assertEquals(expectedCount, htsjdkVariantsRdd.getVariants().count());

    File outputFile = File.createTempFile("test", outputExtension);
    outputFile.delete();
    String outputPath = outputFile.toURI().toString();

    htsjdkVariantsRddStorage.write(htsjdkVariantsRdd, outputPath);

    if (outputExtension.endsWith(".gz") || outputExtension.endsWith(".bgz")) {
      Assert.assertTrue("block compressed", isBlockCompressed(outputFile));
    } else {
      Assert.assertFalse("block compressed", isBlockCompressed(outputFile));
    }
    Assert.assertEquals(expectedCount, getVariantCount(outputFile, null));
  }

  private Object[] parametersForTestReadAndWriteMultiple() {
    return new Object[][] {
      {"HiSeq.10000.vcf.bgz", ".vcfs", 128 * 1024},
      {"HiSeq.10000.vcf.bgz", ".vcf.gzs", 128 * 1024},
      {"HiSeq.10000.vcf.bgz", ".vcf.bgzs", 128 * 1024},
    };
  }

  @Test
  @Parameters
  public void testReadAndWriteMultiple(String inputFile, String outputExtension, int splitSize)
      throws IOException, URISyntaxException {
    String inputPath = ClassLoader.getSystemClassLoader().getResource(inputFile).toURI().toString();

    HtsjdkVariantsRddStorage htsjdkVariantsRddStorage =
        HtsjdkVariantsRddStorage.makeDefault(jsc).splitSize(splitSize);

    HtsjdkVariantsRdd htsjdkVariantsRdd = htsjdkVariantsRddStorage.read(inputPath);

    Assert.assertTrue(htsjdkVariantsRdd.getVariants().getNumPartitions() > 1);

    int expectedCount = getVariantCount(new File(inputPath.replace("file:", "")), null);
    Assert.assertEquals(expectedCount, htsjdkVariantsRdd.getVariants().count());

    File outputFile = File.createTempFile("test", outputExtension);
    outputFile.delete();
    String outputPath = outputFile.toURI().toString();

    htsjdkVariantsRddStorage.write(htsjdkVariantsRdd, outputPath);

    Assert.assertTrue(outputFile.isDirectory());
    int totalCount = 0;
    for (File part : outputFile.listFiles(file -> file.getName().startsWith("part-"))) {
      totalCount += getVariantCount(part, null);
    }
    Assert.assertEquals(expectedCount, totalCount);
  }

  private Object[] parametersForTestBgzfVcfIsSplitIntoMultiplePartitions() {
    return new Object[][] {
      {"HiSeq.10000.vcf.bgz", null, 4},
      {"HiSeq.10000.vcf.bgz", new Interval("chr1", 2700000, 2800000), 1},
      {"HiSeq.10000.vcf.bgzf.gz", null, 4},
    };
  }

  @Test
  @Parameters
  public void testBgzfVcfIsSplitIntoMultiplePartitions(
      String inputFile, Interval interval, int expectedPartitions)
      throws IOException, URISyntaxException {
    String inputPath = ClassLoader.getSystemClassLoader().getResource(inputFile).toURI().toString();

    JavaRDD<VariantContext> variants =
        HtsjdkVariantsRddStorage.makeDefault(jsc)
            .splitSize(128 * 1024)
            .read(inputPath, interval == null ? null : Collections.singletonList(interval))
            .getVariants();

    Assert.assertEquals(expectedPartitions, variants.getNumPartitions());

    int expectedCount = getVariantCount(new File(inputPath.replace("file:", "")), interval);
    Assert.assertEquals(expectedCount, variants.count());
  }

  private static boolean isBlockCompressed(File file) throws IOException {
    try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
      return BlockCompressedInputStream.isValidFile(in);
    }
  }

  private static VCFFileReader parseVcf(File vcf) throws IOException {
    File actualVcf;
    // work around TribbleIndexedFeatureReader not reading header from .bgz files
    if (vcf.getName().endsWith(".bgz")) {
      actualVcf = File.createTempFile(vcf.getName(), ".gz");
      actualVcf.deleteOnExit();
      Files.copy(vcf, actualVcf);
      File tbi = new File(vcf.getParent(), vcf.getName() + TabixUtils.STANDARD_INDEX_EXTENSION);
      if (tbi.exists()) {
        File actualTbi =
            new File(
                actualVcf.getParent(), actualVcf.getName() + TabixUtils.STANDARD_INDEX_EXTENSION);
        actualTbi.deleteOnExit();
        Files.copy(tbi, actualTbi);
      }
    } else {
      actualVcf = vcf;
    }
    return new VCFFileReader(actualVcf, false);
  }

  private static int getVariantCount(final File vcf, Interval interval) throws IOException {
    final VCFFileReader vcfFileReader = parseVcf(vcf);
    final Iterator<VariantContext> it;
    if (interval == null) {
      it = vcfFileReader.iterator();
    } else {
      it = vcfFileReader.query(interval.getContig(), interval.getStart(), interval.getEnd());
    }
    int recCount = 0;
    while (it.hasNext()) {
      it.next();
      recCount++;
    }
    vcfFileReader.close();
    return recCount;
  }
}
