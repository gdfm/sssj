package sssj.base;

public class StreamingMaxVector extends Vector {
  private final double lambda;

  public StreamingMaxVector(double lambda) {
    this.lambda = lambda;
  }

  /**
   * Updates the vector to the max of itself and the vector query, taking into account the forgetting factor.
   * 
   * @param query the new vector
   * @return the subset of the new vector that was larger than maxVector (for reindexing)
   */
  public Vector updateMaxByDimension(Vector query) {
    if (query.timestamp() > this.timestamp())
      this.setTimestamp(query.timestamp());
    return Vector.EMPTY_VECTOR;
  }
}
