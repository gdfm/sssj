package sssj.index.minibatch.component;

import sssj.io.Vector;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;

public class MaxVector extends Vector {

  /**
   * Updates the vector to the max of itself and the vector query. The timestamp of the MaxVector reflects the time it was last updated.
   * 
   * @param query the new vector
   * @return the subset of the new vector that was larger than maxVector (for reindexing)
   */
  public Vector updateMaxByDimension(Vector query) {
    final Vector updates = new Vector(query.timestamp());
    for (Int2DoubleMap.Entry e : query.int2DoubleEntrySet()) {
      final int dimension = e.getIntKey();
      final double weight = e.getDoubleValue();
      if (Double.compare(weight, this.get(dimension)) > 0) {
        this.setTimestamp(query.timestamp());
        this.put(dimension, weight);
        updates.put(dimension, weight);
      }
    }
    return updates;
  }

  public void clear() {
    data.clear();
  }
}
