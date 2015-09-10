package sssj.base;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

/**
 * A buffer for Vectors. The buffer keeps the order of the vectors as they are added, and maintains the maximum vector. Assumes vectors are added in increasing
 * order of timestamp.
 */
public class VectorWindow {
  private Vector max = new Vector(); // TODO for INVERTED index the max is not needed
  private Queue<Vector> queue = new LinkedList<>();
  private final double tau;
  private int epoch;

  public VectorWindow(double tau) {
    this.tau = tau;
    this.epoch = 0;
  }

  /**
   * Add a vector to the current buffer if the vector timestamp lies within the current buffer window. The vector is defensively copied.
   * 
   * @return true if successfully added the vector, false if the vector timestamp is beyond the current window
   */
  public boolean add(Vector v) {
    Preconditions.checkArgument(v.timestamp() >= windowStart());
    max.updateMaxByDimension(v); // update the max vector
    if (v.timestamp() < windowEnd()) { // v is within the time window of 2*tau
      queue.add(new Vector(v)); // copy constructor needed because the vector iterator reuses its instance
      return true;
    } else {
      return false;
    }
  }

  public Vector getMax() {
    return max;
  }

  public VectorWindow slide() {
    this.epoch++;
    while (!queue.isEmpty() && queue.peek().timestamp() < windowStart())
      queue.remove();
    // update the max vector
    max.clear();
    for (Vector v : queue)
      max.updateMaxByDimension(v);
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

  @Override
  public String toString() {
    return "[epoch=" + epoch + ", queue=" + queue + "]";
  }
}
