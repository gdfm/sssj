package sssj;

import java.io.BufferedReader;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import sssj.Utils.BatchResult;
import sssj.Utils.IndexType;
import sssj.index.APIndex;
import sssj.index.Index;
import sssj.index.InvertedIndex;
import sssj.index.L2APIndex;
import sssj.io.VectorStreamReader;

import com.github.gdfm.shobaidogu.IOUtils;

/**
 * Baseline micro-batch method. Keeps a buffer of vectors of length 2*tau. When the buffer is full, index and query the
 * first half of the vectors with a batch index (Inverted, AP, L2AP), and query the index built so far with the second
 * half of the buffer. Discard the first half of the buffer, retain the second half as the new first half, and repeat
 * the process.
 */
public class Baseline {

  public static void main(String[] args) throws Exception {
    System.out.println("Baseline!");
    
    String filename = args[0];
    BufferedReader reader = IOUtils.getBufferedReader(filename);
    VectorStreamReader stream = new VectorStreamReader(reader);

    final double theta = 0.03;
    final double lambda = 1;
    final IndexType idxType = IndexType.INVERTED;
    
    compute(stream, theta, lambda, idxType);
  }

  public static void compute(Iterable<Vector> stream, double theta, double lambda, IndexType idxType) {
    final double tau = Utils.computeTau(theta, lambda);
    VectorBuffer window = new VectorBuffer(tau);

    for (Vector v : stream) {
      boolean inWindow = window.add(v);
      while (!inWindow) {
        if (window.size() > 0)
          computeResults(window, theta, idxType);
        else
          window.slide();
        inWindow = window.add(v);
      }
    }
    // last 2 window slides
    while (!window.isEmpty())
      computeResults(window, theta, idxType);
  }

  private static void computeResults(VectorBuffer window, double theta, IndexType type) {
    // select and initialize index
    Index index = null;
    switch (type) {
    case INVERTED:
      index = new InvertedIndex(theta);
      break;
    case ALL_PAIRS:
      index = new APIndex(theta, window.getMax());
      break;
    case L2AP:
      index = new L2APIndex(theta);
      break;
    }
    assert (index != null);

    // build and query the index on first half of the buffer
    BatchResult res1 = query(index, window.firstHalf(), true);
    // query the index with the second half of the buffer without indexing it
    BatchResult res2 = query(index, window.secondHalf(), false);
    // slide the window
    window.slide();

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
