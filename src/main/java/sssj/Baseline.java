package sssj;

import java.io.BufferedReader;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import sssj.L2APIndex.BatchResult;
import sssj.io.StreamReader;

import com.github.gdfm.shobaidogu.IOUtils;

/**
 * Baseline micro-batch method. Keeps a buffer of vectors of length 2*tau. When the buffer is full, index and query the
 * first half of the vectors with the batch L2AP, and query the index built so far with the second half of the buffer.
 * Discard the first half of the buffer, retain the second half as the new first half, and repeat the process.
 */
public class Baseline {

  public static void main(String[] args) throws Exception {
    System.out.println("Baseline!");
    String filename = args[0];
    final double theta = 0.03;
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
          flushResults(buffer, theta);
        }
        inWindow = buffer.add(v);
      }
    }
    while (!buffer.isEmpty())
      // last 2 window slides
      flushResults(buffer, theta);
  }

  private static void flushResults(VectorBuffer buffer, double theta) {
    //    System.out.println(Iterators.size(buffer.firstHalf()) + " + " + Iterators.size(buffer.secondHalf()) + " = " + buffer.size());
    // build index on first half of the buffer
    L2APIndex index = new L2APIndex(theta);
    BatchResult res1 = buildL2APIndex(index, buffer);
    // query the index with the second half of the buffer
    BatchResult res2 = queryL2APIndex(index, buffer);
    buffer.slide();
    // print results
    for (Entry<Long, Map<Long, Double>> row : res1.rowMap().entrySet()) {
      System.out.println(row.getKey() + ": " + row.getValue());
    }
    for (Entry<Long, Map<Long, Double>> row : res2.rowMap().entrySet()) {
      System.out.println(row.getKey() + ": " + row.getValue());
    }
  }

  private static BatchResult buildL2APIndex(L2APIndex index, VectorBuffer buffer) {
    BatchResult result = new BatchResult();
    for (Iterator<Vector> it = buffer.firstHalf(); it.hasNext();) {
      Vector v = it.next();
      Map<Long, Double> matches = index.queryWith(v);
      for (Entry<Long, Double> e : matches.entrySet()) {
        result.put(v.timestamp(), e.getKey(), e.getValue());
      }
      // Vector maxVector = buffer.getMax();
      index.addVector(v); // index the vector
    }
    return result;
  }

  private static BatchResult queryL2APIndex(L2APIndex index, VectorBuffer buffer) {
    BatchResult result = new BatchResult();
    for (Iterator<Vector> it = buffer.secondHalf(); it.hasNext();) {
      Vector v = it.next();
      Map<Long, Double> matches = index.queryWith(v);
      for (Entry<Long, Double> e : matches.entrySet()) {
        result.put(v.timestamp(), e.getKey(), e.getValue());
      }
    }
    return result;
  }
}
