package sssj.index;

public abstract class AbstractIndex implements Index {
  protected int size;
  protected long numPostingEntries;
  protected long numCandidates;
  protected long numSimilarities;
  protected int numMatches;

  @Override
  public IndexStatistics stats() {
    return new SimpleIndexStatistics(size, numPostingEntries, numCandidates, numSimilarities, numMatches);
  }

  public static class SimpleIndexStatistics implements IndexStatistics {
    private final int sz, nm;
    private final long np, nc, ns;

    public SimpleIndexStatistics(int size, long nPostingEntries, long nCandidates, long nSimilarities, int nMatches) {
      this.sz = size;
      this.np = nPostingEntries;
      this.nc = nCandidates;
      this.ns = nSimilarities;
      this.nm = nMatches;
    }

    @Override
    public int size() {
      return sz;
    }

    @Override
    public long numPostingEntries() {
      return np;
    }

    @Override
    public long numCandidates() {
      return nc;
    }

    @Override
    public long numSimilarities() {
      return ns;
    }

    @Override
    public int numMatches() {
      return nm;
    }
  }
}
