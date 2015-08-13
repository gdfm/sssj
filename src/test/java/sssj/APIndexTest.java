package sssj;

import static org.junit.Assert.*;

import org.junit.Test;

import sssj.index.APIndex;

public class APIndexTest {

  @Test
  public void test() {
    Vector max = new Vector();
    max.put(0, 1.0);
    max.put(1, 1.0);
    APIndex index = new APIndex(0.5, max);
    Vector v = new Vector();
    v.put(0, 0.1);
    v.put(1, 1.0);
    Vector residual = index.addVector(v);
    assertEquals(1, v.size());
    assertTrue(v.containsKey(0));
    assertEquals(0.1, v.get(0), Double.MIN_NORMAL);
    assertEquals(v, residual); // index.addVector() returns the same vector
  }
}