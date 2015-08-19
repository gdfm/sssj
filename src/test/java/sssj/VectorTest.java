package sssj;

import static org.junit.Assert.*;
import it.unimi.dsi.fastutil.BidirectionalIterator;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry;
import it.unimi.dsi.fastutil.ints.IntIterator;

import org.junit.Test;

public class VectorTest {

  @Test
  public void testNotContains() {
    Vector v = new Vector();
    double d = v.get(0);
    assertEquals(0, d, Double.MIN_NORMAL);
  }

  @Test
  public void testIterationOrder() {
    Vector v = new Vector();
    v.put(0, 0);
    v.put(1, 1);
    v.put(2, 2);
    v.put(3, 3);
    v.put(4, 4);
    BidirectionalIterator<Int2DoubleMap.Entry> it = v.int2DoubleEntrySet().fastIterator(v.int2DoubleEntrySet().last());
    //    for (int i = 0; i < v.size(); i++) {
    int i = 4;
    while (it.hasPrevious()) {
      Entry e = it.previous();
      int lastKey = e.getIntKey();
      double lastValue = e.getDoubleValue();
      assertEquals(i, lastKey);
      assertEquals(i, lastValue, Double.MIN_NORMAL);
      i--;
    }
    assertEquals(5, v.size());
    assertEquals(0, v.firstIntKey());
    assertEquals(4, v.lastIntKey());
  }

  @Test
  public void testNormalize() {
    Vector v = new Vector();
    v.put(0, 0.5);
    v.put(1, 0.5);
    v.put(2, 0.5);
    v.put(3, 0.5);
    assertEquals(1, v.magnitude(), Double.MIN_NORMAL);
    Vector n = Vector.l2normalize(v);
    assertEquals(v, n);
    assertEquals(0.5, n.maxValue, Double.MIN_NORMAL);
    assertTrue(v == n);

    v = new Vector();
    v.put(0, 0.5);
    v.put(1, 0.5);
    v.put(2, 0.5);
    n = Vector.l2normalize(v);
    assertEquals(1, n.magnitude(), Double.MIN_NORMAL);
    assertEquals(3, n.size());
    IntIterator it = v.keySet().iterator();
    for (int k : n.keySet()) {
      assertEquals(k, it.nextInt()); // same order
    }
    assertEquals(0.5 / v.magnitude(), n.maxValue(), Double.MIN_NORMAL); // max is updated
  }
}
