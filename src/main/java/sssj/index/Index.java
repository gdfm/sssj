package sssj;

import java.util.Map;

public interface Index {

  /**
   * Queries the current index with the vector passed as parameter.
   * 
   * @param v
   *          the query vector
   * @return the matchings between the previously indexed vectors and the query
   */
  public abstract Map<Long, Double> queryWith(Vector v);

  /**
   * Indexes the vector passed as parameter in the current index.
   * 
   * @param v
   *          the vector to index
   * @return the unindexed part of the vector
   */
  public abstract Vector addVector(Vector v);

}