package org.disq_bio.disq.impl.formats.vcf;

import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;

// TODO: expose header and version fields in VCFCodec in htsjdk
class VersionAwareVCFCodec extends VCFCodec {
  public VCFHeader getHeader() {
    return header;
  }
  public VCFHeaderVersion getVersion() {
    return version;
  }
}
