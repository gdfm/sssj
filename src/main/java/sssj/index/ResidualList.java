package sssj.index;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

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
    Vector result = Iterables.find(queue, Predicates.equalTo(new Vector(candidateID)), null);
    return result;
  }
}
