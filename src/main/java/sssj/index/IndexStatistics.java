package sssj.index;

public interface IndexStatistics {
  /**
   * Returns the size of the index in number of entries (dimension-value pairs).
   * 
   * @return the number of entries indexed
   */
  public int size();

  /**
   * Returns the total number of posting entries scanned.
   * 
   * @return the total number of posting entries scanned
   */
  public long numPostingEntries();

  /**
   * Returns the total number of candidates generated.
   * 
   * @return the total number of candidates generated
   */
  public long numCandidates();

  /**
   * Returns the total number of full similarities computed.
   * 
   * @return the total number of full similarities computed
   */
  public long numSimilarities();
}
