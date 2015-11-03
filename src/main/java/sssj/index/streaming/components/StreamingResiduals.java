package sssj.index.streaming.components;

import it.unimi.dsi.fastutil.longs.Long2ReferenceLinkedOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ReferenceMap.Entry;

import java.util.Iterator;

import sssj.base.Vector;

public class StreamingResiduals implements Iterable<Vector> {
  private Long2ReferenceLinkedOpenHashMap<Vector> map = new Long2ReferenceLinkedOpenHashMap<>();

  public void add(Vector residual) {
    map.put(residual.timestamp(), residual);
  }

  @Override
  public Iterator<Vector> iterator() {
    return map.values().iterator();
  }

  @Override
  public String toString() {
    return "Residuals = [" + map + "]";
  }

  public Vector get(long candidateID) {
    return map.get(candidateID);
  }

  /**
   * Get the residual of the vector with id {@code candidateID}, while at the same time pruning all the residuals with timestamp less than {@code lowWatermark}.
   *
   * @param candidateID the id of the candidate vector
   * @param lowWatermark the minimum id of the vector to be retained
   * @return the residual of the candidate
   */
  public Vector getAndPrune(long candidateID, long lowWatermark) {
    for (Iterator<Entry<Vector>> it = map.long2ReferenceEntrySet().fastIterator(); it.hasNext();) {
      final Vector v = it.next().getValue();
      if (lowWatermark > v.timestamp())
        it.remove();
      // FIXME optimize pruning
      if (candidateID == v.timestamp())
        return v;
    }
    return null;
  }
}