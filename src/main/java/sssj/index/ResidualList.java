package sssj.index;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import sssj.Vector;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

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
    Vector result = Iterables.find(queue, Predicates.equalTo(new Vector(candidateID)), null); //TODO make residual finding more efficient
    return result;
  }
}
