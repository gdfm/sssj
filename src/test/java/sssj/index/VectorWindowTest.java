package sssj.index;

import static org.junit.Assert.*;

import org.junit.Test;

import sssj.base.Vector;
import sssj.index.VectorWindow;

import com.google.common.collect.Iterators;

public class VectorWindowTest {

  @Test
  public void testAddAndSlide() {
    VectorWindow buffer = new VectorWindow(1);
    Vector v0 = new Vector(0);
    Vector v1 = new Vector(1);
    Vector v2 = new Vector(2);
    assertTrue(buffer.add(v0));
    assertTrue(buffer.add(v1));
    assertFalse(buffer.add(v2)); // v2 is not added
    assertEquals(2, buffer.size());
    buffer.slide();
    assertTrue(buffer.add(v2));
    assertEquals(2, buffer.size());
  }

  @Test
  public void testWindow() {
    VectorWindow buffer = new VectorWindow(1.5);
    assertEquals(0, buffer.windowStart(), Double.MIN_NORMAL);
    assertEquals(3, buffer.windowEnd(), Double.MIN_NORMAL);
    buffer.slide().slide();
    assertEquals(3, buffer.windowStart(), Double.MIN_NORMAL);
    assertEquals(6, buffer.windowEnd(), Double.MIN_NORMAL);
  }

  @Test
  public void testIterators() {
    VectorWindow buffer = new VectorWindow(2);
    for (int i = 0; i < 4; i++)
      assertTrue(buffer.add(new Vector(i)));
    assertTrue(buffer.add(new Vector(3))); // duplicate vector
    assertFalse(buffer.add(new Vector(4)));
    assertEquals(5, buffer.size());
    assertEquals(2, Iterators.size(buffer.firstHalf()));
    assertEquals(3, Iterators.size(buffer.secondHalf()));
    buffer.slide();
    assertEquals(3, Iterators.size(buffer.firstHalf()));
    assertEquals(0, Iterators.size(buffer.secondHalf()));
  }

  @Test
  public void testMax() {
    VectorWindow buffer = new VectorWindow(1);
    Vector v0 = new Vector(0);
    Vector v1 = new Vector(1);
    Vector v2 = new Vector(2);
    v0.put(1, 1.0);
    v1.put(1, 0.5);
    v2.put(1, 0.3);
    assertTrue(buffer.add(v0));
    assertEquals(1.0, buffer.getMax().get(1), Double.MIN_NORMAL);
    assertTrue(buffer.add(v1));
    assertEquals(1.0, buffer.getMax().get(1), Double.MIN_NORMAL);
    buffer.slide();
    assertEquals(0.5, buffer.getMax().get(1), Double.MIN_NORMAL);
    assertTrue(buffer.add(v2));
    assertEquals(0.5, buffer.getMax().get(1), Double.MIN_NORMAL);
    buffer.slide();
    assertEquals(0.3, buffer.getMax().get(1), Double.MIN_NORMAL);
    buffer.slide();
    assertEquals(0, buffer.getMax().get(1), Double.MIN_NORMAL);
    assertEquals(0, buffer.size());
  }
}
