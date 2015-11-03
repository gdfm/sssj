package sssj.index.minibatch.component;

import it.unimi.dsi.fastutil.longs.Long2ReferenceOpenHashMap;
import sssj.io.Vector;

public class Residuals {
  private final Long2ReferenceOpenHashMap<Vector> map = new Long2ReferenceOpenHashMap<>();

  public void add(Vector residual) {
    map.put(residual.timestamp(), residual);
  }

  public Vector get(long candidateID) {
    return map.get(candidateID);
  }

  @Override
  public String toString() {
    return "Residuals = [" + map + "]";
  }
}