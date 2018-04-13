package com.tom_e_white.squark;

import com.google.common.io.Files;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
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

    int expectedCount = getVariantCount(new File(inputPath.replace("file:", "")));
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
    Assert.assertEquals(expectedCount, getVariantCount(outputFile));
  }

  @Test
  public void testBgzfVcfIsSplitIntoMultiplePartitions() throws IOException, URISyntaxException {
    String inputPath =
        ClassLoader.getSystemClassLoader()
            .getResource("HiSeq.10000.vcf.bgzf.gz")
            .toURI()
            .toString();

    JavaRDD<VariantContext> variants =
        HtsjdkVariantsRddStorage.makeDefault(jsc)
            .splitSize(128 * 1024)
            .read(inputPath)
            .getVariants();

    Assert.assertTrue(variants.getNumPartitions() > 1);

    int expectedCount = getVariantCount(new File(inputPath.replace("file:", "")));
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
    } else {
      actualVcf = vcf;
    }
    return new VCFFileReader(actualVcf, false);
  }

  private static int getVariantCount(final File vcf) throws IOException {
    final VCFFileReader vcfFileReader = parseVcf(vcf);
    final Iterator<VariantContext> it = vcfFileReader.iterator();
    int recCount = 0;
    while (it.hasNext()) {
      it.next();
      recCount++;
    }
    vcfFileReader.close();
    return recCount;
  }
}
