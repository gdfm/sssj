package sssj;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class ResidualList implements Iterable<Vector> {
  private Queue<Vector> queue = new LinkedList<>();

  public void add(Vector residual) {
    queue.add(residual);
  }

  @Override
  public Iterator<Vector> iterator() {
    return queue.iterator();
  }
}
