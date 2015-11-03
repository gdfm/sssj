package sssj.index;

public enum IndexType {
  INVERTED(false), ALLPAIRS(true), L2AP(true), PUREL2AP(false);

  IndexType(boolean needsMax) {
    this.needsMax = needsMax;
  }

  public boolean needsMax() {
    return needsMax;
  }

  private final boolean needsMax;
}