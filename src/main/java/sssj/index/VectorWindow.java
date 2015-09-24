package sssj.index;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

import sssj.base.MaxVector;
import sssj.base.Vector;

import com.google.common.base.Preconditions;

/**
 * A buffer for Vectors. The buffer keeps the order of the vectors as they are added, and maintains the maximum vector. Assumes vectors are added in increasing
 * order of timestamp.
 */
public class VectorWindow {
  private MaxVector max1 = new MaxVector();
  private MaxVector max2 = new MaxVector();
  private Queue<Vector> q1 = new ArrayDeque<>();
  private Queue<Vector> q2 = new ArrayDeque<>();
  private final double tau;
  private final boolean keepMax;
  private int epoch;

  public VectorWindow(double tau, boolean keepMax) {
    this.tau = tau;
    this.keepMax = keepMax;
    this.epoch = 0;
  }

  public VectorWindow(double tau) {
    this(tau, true);
  }

  /**
   * Add a vector to the current buffer if the vector timestamp lies within the current buffer window. The vector is defensively copied.
   * 
   * @return true if successfully added the vector, false if the vector timestamp is beyond the current window
   */
  public boolean add(Vector v) {
    Preconditions.checkArgument(v.timestamp() >= windowStart());
    if (v.timestamp() < windowEnd()) { // v is within the time window of 2*tau
      if (keepMax)
        max1.updateMaxByDimension(v); // update max1 vector
      // copy constructor needed because the vector iterator reuses its instance
      if (v.timestamp() < windowMid()) {
        q1.add(new Vector(v));
      } else {
        // v.timestamp() >= windowMid()
        q2.add(new Vector(v));
        if (keepMax)
          max2.updateMaxByDimension(v); // update max2 vector
      }
      return true;
    } else {
      return false;
    }
  }

  public MaxVector getMax() {
    if (keepMax)
      return max1;
    throw new UnsupportedOperationException("Window initialized not to track maximum");
  }

  public VectorWindow slide() {
    epoch++;
    // swap the queues
    Queue<Vector> tmpq = q1;
    q1 = q2;
    q2 = tmpq;
    q2.clear();
    if (keepMax) {
      // swap the max vectors
      MaxVector tmpmax = max1;
      max1 = max2;
      max2 = tmpmax;
      max2.clear();
    }
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
    return q1.size() + q2.size();
  }

  public Iterator<Vector> firstHalf() {
    return q1.iterator();
  }

  public Iterator<Vector> secondHalf() {
    return q2.iterator();
  }

  public boolean isEmpty() {
    return this.size() == 0;
  }

  @Override
  public String toString() {
    return "[epoch=" + epoch + ", q1=" + q1 + ", q2=" + q2 + "]";
  }
}
