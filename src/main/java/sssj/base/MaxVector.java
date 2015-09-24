package sssj.base;

import it.unimi.dsi.fastutil.ints.Int2DoubleMap;

public class MaxVector extends Vector {

  /**
   * Updates the vector to the max of itself and the vector query.
   * 
   * @param query the new vector
   * @return the subset of the new vector that was larger than maxVector (for reindexing)
   */
  public Vector updateMaxByDimension(Vector query) {
    final Vector updates = new Vector(query.timestamp);
    for (Int2DoubleMap.Entry e : query.int2DoubleEntrySet()) {
      if (Double.compare(this.get(e.getIntKey()), e.getDoubleValue()) < 0) {
        this.put(e.getIntKey(), e.getDoubleValue());
        updates.put(e.getIntKey(), e.getDoubleValue());
        this.setTimestamp(query.timestamp()); // FIXME can have a timestamp per dimension
      }
    }
    return updates;
  }

  public void clear() {
    data.clear();
  }
}
