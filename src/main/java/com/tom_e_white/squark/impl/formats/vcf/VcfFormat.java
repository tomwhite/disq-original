package com.tom_e_white.squark.impl.formats.vcf;

import com.tom_e_white.squark.HtsjdkVariantsRddStorage;

public enum VcfFormat {
  VCF(".vcf", false),
  VCF_BGZ(".vcf.bgz", true),
  VCF_GZ(".vcf.gz", true);

  private String extension;
  private boolean compressed;

  VcfFormat(String extension, boolean compressed) {
    this.extension = extension;
    this.compressed = compressed;
  }

  public String getExtension() {
    return extension;
  }

  public String getIndexExtension() {
    return extension + ".tbi";
  }

  public boolean fileMatches(String path) {
    return path.endsWith(extension);
  }

  public boolean isCompressed() {
    return compressed;
  }

  public HtsjdkVariantsRddStorage.FormatWriteOption toFormatWriteOption() {
    return HtsjdkVariantsRddStorage.FormatWriteOption.valueOf(
        name()); // one-to-one correspondence between names
  }

  public static VcfFormat fromFormatWriteOption(
      HtsjdkVariantsRddStorage.FormatWriteOption formatWriteOption) {
    return valueOf(formatWriteOption.name());
  }

  public static VcfFormat fromExtension(String extension) {
    for (VcfFormat format : values()) {
      if (extension.equals(format.extension)) {
        return format;
      }
    }
    return null;
  }

  public static VcfFormat fromPath(String path) {
    for (VcfFormat format : values()) {
      if (path.endsWith(format.extension)) {
        return format;
      }
    }
    return null;
  }
}
