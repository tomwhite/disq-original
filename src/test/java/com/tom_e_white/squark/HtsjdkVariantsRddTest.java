package com.tom_e_white.squark;

import static com.tom_e_white.squark.VcfTestUtil.countVariants;

import com.tom_e_white.squark.HtsjdkVariantsRddStorage.FormatWriteOption;
import com.tom_e_white.squark.impl.formats.vcf.VcfFormat;
import htsjdk.samtools.util.Interval;
import htsjdk.variant.variantcontext.VariantContext;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
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
      {"test.vcf", FormatWriteOption.VCF, 128 * 1024},
      {"test.vcf", FormatWriteOption.VCF_GZ, 128 * 1024},
      {"test.vcf", FormatWriteOption.VCF_BGZ, 128 * 1024},
      {"test.vcf.bgz", FormatWriteOption.VCF, 128 * 1024},
      {"test.vcf.bgzf.gz", FormatWriteOption.VCF, 128 * 1024},
      {"test.vcf.gz", FormatWriteOption.VCF, 128 * 1024},
    };
  }

  @Test
  @Parameters
  public void testReadAndWrite(String inputFile, FormatWriteOption formatWriteOption, int splitSize)
      throws IOException, URISyntaxException {
    String inputPath = getPath(inputFile);
    VcfFormat outputVcfFormat = VcfFormat.fromFormatWriteOption(formatWriteOption);

    HtsjdkVariantsRddStorage htsjdkVariantsRddStorage =
        HtsjdkVariantsRddStorage.makeDefault(jsc).splitSize(splitSize);

    HtsjdkVariantsRdd htsjdkVariantsRdd = htsjdkVariantsRddStorage.read(inputPath);

    // read the file using htsjdk to get expected number of reads, then count the number in the RDD
    int expectedCount = countVariants(inputPath);
    Assert.assertEquals(expectedCount, htsjdkVariantsRdd.getVariants().count());

    // write the RDD back to a file
    String outputPath = createTempPath(outputVcfFormat.getExtension());
    htsjdkVariantsRddStorage.write(htsjdkVariantsRdd, outputPath);

    // check the new file has the number of expected variants
    Assert.assertEquals(
        "block compressed",
        outputVcfFormat.isCompressed(),
        VcfTestUtil.isBlockCompressed(outputPath));
    Assert.assertEquals(expectedCount, countVariants(outputPath));
    if (BcftoolsTestUtil.isBcftoolsAvailable()) {
      Assert.assertEquals(expectedCount, BcftoolsTestUtil.countVariants(outputPath));
    }

    // check we can read back what we've just written
    Assert.assertEquals(
        expectedCount, htsjdkVariantsRddStorage.read(outputPath).getVariants().count());
  }

  private Object[] parametersForTestReadAndWriteMultiple() {
    return new Object[][] {
      {"HiSeq.10000.vcf.bgz", 128 * 1024, FormatWriteOption.VCF},
      {"HiSeq.10000.vcf.bgz", 128 * 1024, FormatWriteOption.VCF_GZ},
      {"HiSeq.10000.vcf.bgz", 128 * 1024, FormatWriteOption.VCF_BGZ},
    };
  }

  @Test
  @Parameters
  public void testReadAndWriteMultiple(
      String inputFile, int splitSize, FormatWriteOption formatWriteOption)
      throws IOException, URISyntaxException {
    String inputPath = getPath(inputFile);

    HtsjdkVariantsRddStorage htsjdkVariantsRddStorage =
        HtsjdkVariantsRddStorage.makeDefault(jsc).splitSize(splitSize);

    HtsjdkVariantsRdd htsjdkVariantsRdd = htsjdkVariantsRddStorage.read(inputPath);

    // check that there are multiple partitions
    Assert.assertTrue(htsjdkVariantsRdd.getVariants().getNumPartitions() > 1);

    // read the file using htsjdk to get expected number of variants, then count the number in the
    // RDD
    int expectedCount = countVariants(inputPath);
    Assert.assertEquals(expectedCount, htsjdkVariantsRdd.getVariants().count());

    // write as multiple VCF files
    String outputPath = createTempPath("");
    htsjdkVariantsRddStorage.write(
        htsjdkVariantsRdd,
        outputPath,
        HtsjdkVariantsRddStorage.FileCardinalityWriteOption.MULTIPLE,
        formatWriteOption);

    // check the new file has the number of expected variants
    int totalCount = 0;
    for (String part : listPartFiles(outputPath)) {
      totalCount += countVariants(part);
    }
    Assert.assertEquals(expectedCount, totalCount);

    if (BcftoolsTestUtil.isBcftoolsAvailable()) {
      int totalCountBcftools = 0;
      for (String part : listPartFiles(outputPath)) {
        totalCountBcftools += BcftoolsTestUtil.countVariants(part);
      }
      Assert.assertEquals(expectedCount, totalCountBcftools);
    }

    // check we can read back what we've just written
    Assert.assertEquals(
        expectedCount, htsjdkVariantsRddStorage.read(outputPath).getVariants().count());
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
    String inputPath = getPath(inputFile);

    JavaRDD<VariantContext> variants =
        HtsjdkVariantsRddStorage.makeDefault(jsc)
            .splitSize(128 * 1024)
            .read(inputPath, interval == null ? null : Collections.singletonList(interval))
            .getVariants();

    Assert.assertEquals(expectedPartitions, variants.getNumPartitions());

    int expectedCount = countVariants(inputPath, interval);
    Assert.assertEquals(expectedCount, variants.count());
  }

  @Test
  public void testOverwrite() throws IOException, URISyntaxException {
    String inputPath = getPath("test.vcf");

    HtsjdkVariantsRddStorage htsjdkVariantsRddStorage =
        HtsjdkVariantsRddStorage.makeDefault(jsc).splitSize(128 * 1024);

    HtsjdkVariantsRdd htsjdkVariantsRdd = htsjdkVariantsRddStorage.read(inputPath);
    int originalCount = countVariants(inputPath);

    // sample the variants
    JavaRDD<VariantContext> sample = htsjdkVariantsRdd.getVariants().sample(false, 0.5);
    HtsjdkVariantsRdd htsjdkVariantsSampleRdd =
        new HtsjdkVariantsRdd(htsjdkVariantsRdd.getHeader(), sample);

    // overwrite input with smaller sample
    htsjdkVariantsRddStorage.write(htsjdkVariantsSampleRdd, inputPath);
    int newCount = countVariants(inputPath);

    Assert.assertTrue(newCount > 0);
    Assert.assertTrue(originalCount > newCount);
  }
}
