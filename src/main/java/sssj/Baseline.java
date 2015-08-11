package sssj;

import java.io.BufferedReader;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import sssj.L2APIndex.L2APResult;
import sssj.io.StreamReader;

import com.github.gdfm.shobaidogu.IOUtils;
import com.google.common.collect.Iterators;

/**
 * Baseline micro-batch method. Keeps a buffer of vectors of length 2*tau. When the buffer is full, index and query the
 * first half of the vectors with the batch L2AP, and query the index built so far with the second half of the buffer.
 * Discard the first half of the buffer, retain the second half as the new first half, and repeat the process.
 */
public class Baseline {

  public static void main(String[] args) throws Exception {
    System.out.println("Baseline!");
    String filename = args[0];
    final double theta = 0.3;
    final double lambda = 1;
    BufferedReader reader = IOUtils.getBufferedReader(filename);
    StreamReader stream = new StreamReader(reader);
    double tau = Utils.computeTau(lambda, theta);
    System.out.println(tau);

    VectorBuffer buffer = new VectorBuffer(tau);

    long currentTimestamp = -1, previousTimestamp = -1;
    for (Vector v : stream) {
      previousTimestamp = currentTimestamp;
      currentTimestamp = v.timestamp();
      boolean inWindow = buffer.add(v);
      while (!inWindow) {
        if (buffer.size() > 0) {
          flushResults(buffer);
        }
        inWindow = buffer.add(v);
      }
    }
    while (!buffer.isEmpty())
      // last 2 window slides
      flushResults(buffer);
  }

  private static void flushResults(VectorBuffer buffer) {
    System.out.println(Iterators.size(buffer.firstHalf()) + " + " + Iterators.size(buffer.secondHalf()) + " = " + buffer.size());
    // build index on first half of the buffer
    L2APIndex index = new L2APIndex();
    Vector maxVector = buffer.getMax();
    Iterator<Vector> firstHalf = buffer.firstHalf();
    L2APResult res1 = index.buildIndex(maxVector, firstHalf);
    // query the index with the second half of the buffer
    queryL2APIndex(index, buffer);
    buffer.slide();
    // print results
    for (Entry<Long, Map<Long, Double>> row : res1.rowMap().entrySet()) {
      System.out.println(row.getKey() + ": " + row.getValue());
    }
  }

  private static L2APResult queryL2APIndex(L2APIndex index, VectorBuffer buffer) {
    return null;
  }

}
