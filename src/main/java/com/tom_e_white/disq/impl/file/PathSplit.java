package com.tom_e_white.disq.impl.file;

public class PathSplit {
  private final String path;
  private final long start;
  private final long end;

  public PathSplit(String path, long start, long end) {
    this.path = path;
    this.start = start;
    this.end = end;
  }

  public String getPath() {
    return path;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }
}
