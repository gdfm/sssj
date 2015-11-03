package sssj.index.streaming.component;

import it.unimi.dsi.fastutil.longs.Long2ReferenceLinkedOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ReferenceMap.Entry;

import java.util.Iterator;

import sssj.io.Vector;

public class StreamingResiduals implements Iterable<Vector> {
  private Long2ReferenceLinkedOpenHashMap<Vector> map = new Long2ReferenceLinkedOpenHashMap<>();

  @Override
  public Iterator<Vector> iterator() {
    return map.values().iterator();
  }

  public void add(Vector residual) {
    map.put(residual.timestamp(), residual);
  }

  /**
   * Get the residual of the vector with id {@code candidateID}, while at the same time pruning all the residuals with timestamp less than {@code lowWatermark}.
   *
   * @param candidateID the id of the candidate vector
   * @param lowWatermark the minimum id of the vector to be retained
   * @return the residual of the candidate
   */
  public Vector getAndPrune(long candidateID, long lowWatermark) {
    Iterator<Entry<Vector>> it = map.long2ReferenceEntrySet().fastIterator();
    while (it.hasNext() && it.next().getValue().timestamp() < lowWatermark) {
      it.remove();
    }
    return map.get(candidateID);
  }

  @Override
  public String toString() {
    return "Residuals = [" + map + "]";
  }
}