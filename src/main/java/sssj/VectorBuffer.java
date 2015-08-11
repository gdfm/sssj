package sssj;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

/**
 * A buffer for Vectors. The buffer keeps the order of the vectors as they are added, and maintains the maximum vector.
 * Assumes vectors are added in increasing order of timestamp.
 */
public class VectorBuffer {
  private Vector max = new Vector();
  private Queue<Vector> queue = new LinkedList<>();
  private final double tau;
  private int epoch;

  public VectorBuffer(double tau) {
    this.tau = tau;
    this.epoch = 0;
  }

  /**
   * Add a vector to the current buffer if the vector timestamp lies within the current buffer window.
   * 
   * @return true if successfully added the vector, false if the vector timestamp is beyond the current window
   */
  public boolean add(Vector v) {
    Preconditions.checkArgument(v.timestamp() >= windowStart());
    Vector.maxByDimension(max, v); // update the max vector
    if (v.timestamp() < windowEnd()) { // v is within the time window of 2*tau
      queue.add(v);
      return true;
    }
    else
      return false;
  }

  public Vector getMax() {
    return max;
  }

  public VectorBuffer slide() {
    this.epoch++;
    while (!queue.isEmpty() && queue.peek().timestamp() < windowStart())
      queue.remove();
    // update the max vector
    this.max.clear();
    for (Vector v : queue)
      Vector.maxByDimension(max, v);
    return this;
  }

  public double windowStart() {
    return epoch * tau;
  }

  public double windowEnd() {
    return (epoch + 2) * tau;
  }

  public double windowMid() {
    return (epoch + 1) * tau;
  }

  public int size() {
    return queue.size();
  }

  public Iterator<Vector> firstHalf() {
    return Iterators.filter(queue.iterator(), new Predicate<Vector>() {
      @Override
      public boolean apply(Vector input) {
        return input.timestamp() < windowMid();
      }
    });
  }

  public Iterator<Vector> secondHalf() {
    return Iterators.filter(queue.iterator(), new Predicate<Vector>() {
      @Override
      public boolean apply(Vector input) {
        return input.timestamp() >= windowMid();
      }
    });
  }

  public boolean isEmpty() {
    return this.size() == 0;
  }
}
