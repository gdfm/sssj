package sssj.index.streaming.component;

import static sssj.util.Commons.forgettingFactor;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import sssj.io.Vector;

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
   */
  public void updateMaxByDimensionFF(Vector query) {
    for (Int2DoubleMap.Entry e : query.int2DoubleEntrySet()) {
      final int dimension = e.getIntKey();
      final double weight = e.getDoubleValue();
      final double dimFF = dimensionFF(dimension, query.timestamp());
      if (Double.compare(weight, this.get(dimension) * dimFF) > 0) {
        this.put(dimension, weight);
        this.setTimestamp(query.timestamp());
        this.lastUpdated.put(dimension, query.timestamp());
      }
    }
  }

  public double simimarityFF(Vector query) {
    double result = 0;
    for (Int2DoubleMap.Entry e : query.int2DoubleEntrySet()) {
      final int dimension = e.getIntKey();
      final double weight = e.getDoubleValue();
      final double dimFF = dimensionFF(dimension, query.timestamp());
      result += weight * this.get(dimension) * dimFF;
    }
    return result;
  }

  public double dimensionFF(int dimension, long queryTimestamp) {
    final long dimDeltaT = queryTimestamp - lastUpdated.get(dimension);
    final double dimFF = forgettingFactor(lambda, dimDeltaT);
    return dimFF;
  }
}
