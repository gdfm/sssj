package sssj;

import static org.junit.Assert.*;

import org.junit.Test;

import com.google.common.collect.Iterators;

public class VectorBufferTest {

  @Test
  public void testAddAndSlide() {
    VectorBuffer buffer = new VectorBuffer(1);
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
    VectorBuffer buffer = new VectorBuffer(1.5);
    assertEquals(0, buffer.windowStart(), Double.MIN_VALUE);
    assertEquals(3, buffer.windowEnd(), Double.MIN_VALUE);
    buffer.slide().slide();
    assertEquals(3, buffer.windowStart(), Double.MIN_VALUE);
    assertEquals(6, buffer.windowEnd(), Double.MIN_VALUE);
  }

  @Test
  public void testIterators() {
    VectorBuffer buffer = new VectorBuffer(2);
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
}
