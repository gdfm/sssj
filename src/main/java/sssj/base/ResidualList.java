package sssj.base;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class ResidualList implements Iterable<Vector> {
  private Queue<Vector> queue = new LinkedList<>(); // TODO use ArrayDequeue?

  public void add(Vector residual) {
    queue.add(residual);
  }

  @Override
  public Iterator<Vector> iterator() {
    return queue.iterator();
  }

  @Override
  public String toString() {
    return "ResidualList = [" + queue + "]";
  }

  public Vector get(long candidateID) {
    for (Vector v : queue)
      if (candidateID == v.timestamp())
        return v;
    return null;
  }

  /**
   * Get the residual of the vector with id {@code candidateID}, while at the same time pruning all the residuals with timestamp less than {@code lowWatermark}.
   *
   * @param candidateID the id of the candidate vector
   * @param lowWatermark the minimum id of the vector to be retained
   * @return the residual of the candidate
   */
  public Vector getAndPrune(long candidateID, long lowWatermark) {
    for (Iterator<Vector> it = queue.iterator(); it.hasNext();) {
      final Vector v = it.next();
      if (lowWatermark > v.timestamp())
        it.remove();
      if (candidateID == v.timestamp())
        return v;
    }
    return null;
  }
}