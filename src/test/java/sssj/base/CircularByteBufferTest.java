package sssj.base;

import static org.junit.Assert.*;

import org.junit.Test;

public class CircularByteBufferTest {

  @Test
  public void testSimple() {
    CircularBuffer b = new CircularBuffer(1);
    b.pushLong(0).pushLong(1).pushLong(2);
    assertEquals(3, b.size());
    assertEquals(4, b.capacity());
    assertEquals(0, b.popLong());
    assertEquals(1, b.popLong());
    assertEquals(2, b.popLong());
    assertEquals(0, b.size());
    assertEquals(4, b.capacity());
  }

  @Test
  public void testInterleave() {
    CircularBuffer b = new CircularBuffer(1);
    for (int i = 0; i < 10; i++) {
      for (int j = i; j < i; j++) {
        b.pushLong(i * j);
      }
      for (int j = i; j < i; j++) {
        assertEquals(i * j, b.popLong());
      }
    }
    assertEquals(0, b.size());
  }

  @Test
  public void testShiftAround() {
    CircularBuffer b = new CircularBuffer(4);
    b.pushLong(7);
    for (int i = 0; i < 10; i++) {
      b.pushLong(7).pushLong(7);
      assertEquals(7 * 7, b.popLong() * b.popLong());
    }
    assertEquals(7, b.popLong());
    assertEquals(0, b.size());
    assertEquals(4, b.capacity());
  }

  @Test
  public void testPeek() {
    CircularBuffer b = new CircularBuffer(4);
    b.pushLong(1).pushLong(2).pushLong(3);
    assertEquals(2, b.peekLong(1));
    assertEquals(3, b.size());
    b.pushLong(4).pushLong(5).pushLong(6);
    assertEquals(5, b.peekLong(4));
    assertEquals(6, b.size());
    assertEquals(1, b.popLong());
    assertEquals(2, b.popLong());
    assertEquals(3, b.popLong());
    assertEquals(3, b.size());
    assertEquals(4, b.peekLong(0));
    assertEquals(6, b.peekLong(2));
    b.pushLong(1).pushLong(2).pushLong(3);
    assertEquals(6, b.size());
    assertEquals(2, b.peekLong(4));
    assertEquals(4, b.popLong());
    assertEquals(5, b.popLong());
    assertEquals(6, b.popLong());
  }
}
