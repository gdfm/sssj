package sssj.index;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import sssj.Vector;

public class ResidualList implements Iterable<Vector> {
  private Queue<Vector> queue = new LinkedList<>();

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
}
