package sssj.base;

import static sssj.base.Commons.forgettingFactor;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;

public class StreamingMaxVector extends Vector {
  private final Int2LongOpenHashMap lastUpdated = new Int2LongOpenHashMap();
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
    final Vector updates = new Vector(query.timestamp());
    for (Int2DoubleMap.Entry e : query.int2DoubleEntrySet()) {
      final int dimension = e.getIntKey();
      final double weight = e.getDoubleValue();
      final long dimDeltaT = query.timestamp() - lastUpdated.get(dimension);
// final double dimFF = forgettingFactor(lambda, dimDeltaT);
      if (Double.compare(weight, this.get(dimension)) > 0) {
// if (Double.compare(weight, this.get(dimension) * dimFF) > 0) { // FIXME the multiplication makes the method too slow
        this.put(dimension, weight);
        this.setTimestamp(query.timestamp());
        this.lastUpdated.put(dimension, query.timestamp());
        updates.put(dimension, weight);
      }
    }
    return updates;
  }
}
