package sssj;

import static org.junit.Assert.*;

import org.junit.Test;

public class VectorTest {

  @Test
  public void testNotContains() {
    Vector v = new Vector();
    double d = v.get(0);
    assertTrue(d == 0.0);
  }

}
