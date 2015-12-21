package sssj.index;

public enum IndexType {
  INV(false), AP(true), L2AP(true), L2(false);

  IndexType(boolean needsMax) {
    this.needsMax = needsMax;
  }

  public boolean needsMax() {
    return needsMax;
  }

  private final boolean needsMax;
}