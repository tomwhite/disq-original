package com.tom_e_white.squark.impl.formats.bgzf;

// TODO: move to htsjdk BlockCompressedFilePointerUtil
public class BgzfVirtualFilePointerUtil {
  public static long makeFilePointer(long blockAddress) {
    return makeFilePointer(blockAddress, 0);
  }

  public static long makeFilePointer(long blockAddress, int blockOffset) {
    return blockAddress << 16 | blockOffset;
  }
}
