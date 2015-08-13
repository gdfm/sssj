package sssj;

import java.io.BufferedReader;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import sssj.index.Index;
import sssj.index.InvertedIndex;
import sssj.index.L2APIndex.BatchResult;
import sssj.io.StreamReader;

import com.github.gdfm.shobaidogu.IOUtils;

/**
 * Baseline micro-batch method. Keeps a buffer of vectors of length 2*tau. When the buffer is full, index and query the
 * first half of the vectors with a batch index (Inverted, AP, L2AP), and query the index built so far with the second half of the buffer.
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
          computeResults(buffer, theta);
        }
        inWindow = buffer.add(v);
      }
    }
    while (!buffer.isEmpty())
      // last 2 window slides
      computeResults(buffer, theta);
  }

  private static void computeResults(VectorBuffer buffer, double theta) {
    // select and initialize index
    Index index = new InvertedIndex(theta);
    //    Index index = new L2APIndex(theta);
    //    Index index = new APIndex(theta, buffer.getMax());
    
    // build and query the index on first half of the buffer
    BatchResult res1 = query(index, buffer.firstHalf(), true);
    // query the index with the second half of the buffer without indexing it
    BatchResult res2 = query(index, buffer.secondHalf(), false);
    buffer.slide();

    // print results
    for (Entry<Long, Map<Long, Double>> row : res1.rowMap().entrySet()) {
      System.out.println(row.getKey() + ": " + row.getValue());
    }
    for (Entry<Long, Map<Long, Double>> row : res2.rowMap().entrySet()) {
      System.out.println(row.getKey() + ": " + row.getValue());
    }
  }

  private static BatchResult query(Index index, Iterator<Vector> iterator, boolean buildIndex) {
    BatchResult result = new BatchResult();
    while (iterator.hasNext()) {
      Vector v = iterator.next();
      Map<Long, Double> matches = index.queryWith(v);
      for (Entry<Long, Double> e : matches.entrySet()) {
        result.put(v.timestamp(), e.getKey(), e.getValue());
      }
      if (buildIndex)
        index.addVector(v); // index the vector
    }
    return result;
  }
}
