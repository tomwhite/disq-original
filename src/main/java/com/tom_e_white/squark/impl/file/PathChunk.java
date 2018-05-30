package com.tom_e_white.squark.impl.file;

import htsjdk.samtools.Chunk;
import java.io.Serializable;

/** Stores the virtual span of a partition for a file path. */
public class PathChunk implements Serializable {
  private final String path;
  private final Chunk span;

  public PathChunk(String path, Chunk span) {
    this.path = path;
    this.span = span;
  }

  public String getPath() {
    return path;
  }

  public Chunk getSpan() {
    return span;
  }
}
