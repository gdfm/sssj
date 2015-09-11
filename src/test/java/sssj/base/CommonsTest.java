package sssj.base;

import org.apache.commons.math3.util.FastMath;
import org.junit.Test;

@SuppressWarnings("unused")
public class CommonsTest {

// @Test
  public void testExpSpeed() {
    long start, finish;
    int i, N = 1_000_000;
    final double l = 0.1;
    double d;

    start = System.currentTimeMillis();
    Commons.precomputeFFTable(l, N);
    long mid = System.currentTimeMillis();
    for (d = i = 0; i < N; i++) {
      d += Commons.eTable(i);
    }
    finish = System.currentTimeMillis();

    System.out.println("Precompute.exp() - Total time taken: " + (finish - start) + " ms.");
    System.out.println("Precompute.exp() - Precompute time: " + (mid - start) + " ms.");
    System.out.println("Precompute.exp() - Compute time: " + (finish - mid) + " ms.");
    System.out.println(d);

    start = System.currentTimeMillis();
    for (d = i = 0; i < N; i++) {
      d += FastMath.exp(-l * i);
    }
    finish = System.currentTimeMillis();
    System.out.println("FastMath.exp() - Total time taken: " + (finish - start) + " ms.");
    System.out.println(d);

    start = System.currentTimeMillis();
    for (d = i = 0; i < N; i++) {
      d += Math.exp(-l * i);
    }
    finish = System.currentTimeMillis();
    System.out.println("Math.exp() - Total time taken: " + (finish - start) + " ms.");
    System.out.println(d);
  }
}
