package sssj.index;

public interface IndexStatistics {
  /**
   * Returns the size of the index in number of entries (dimension-value pairs).
   * 
   * @return the number of entries indexed
   */
  public int size();

  /**
   * Returns the maximum length of the posting lists in the index.
   * 
   * @return the maximum posting list length
   */
  public int maxLength();
  
  public long numCandidates();
  
  public long numSimilarities();
}
