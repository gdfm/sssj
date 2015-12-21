package sssj;

import static sssj.util.Commons.*;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sssj.index.AbstractIndex.SimpleIndexStatistics;
import sssj.index.Index;
import sssj.index.IndexStatistics;
import sssj.index.IndexType;
import sssj.index.minibatch.APIndex;
import sssj.index.minibatch.INVIndex;
import sssj.index.minibatch.L2APIndex;
import sssj.index.minibatch.L2Index;
import sssj.index.minibatch.component.VectorWindow;
import sssj.io.Format;
import sssj.io.Vector;
import sssj.io.VectorStream;
import sssj.io.VectorStreamFactory;
import sssj.time.Timeline.Sequential;
import sssj.util.Commons;

import com.github.gdfm.shobaidogu.ProgressTracker;
import com.google.common.base.Joiner;
import com.google.common.collect.ForwardingTable;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;

/**
 * MiniBatch method. Keeps a buffer of vectors of length 2*tau. When the buffer is full, index and query the first half of the vectors with a batch
 * index (INV, AP, L2AP, L2), and query the index built so far with the second half of the buffer. Discard the first half of the buffer, retain the
 * second half as the new first half, and repeat the process.
 */
public class MiniBatch {
  private static final String ALGO = "MiniBatch";
  private static final Logger log = LoggerFactory.getLogger(MiniBatch.class);

  public static void main(String[] args) throws Exception {
    ArgumentParser parser = ArgumentParsers.newArgumentParser(ALGO).description("SSSJ in " + ALGO + " mode.")
        .defaultHelp(true);
    parser.addArgument("-t", "--theta").metavar("theta").type(Double.class).choices(Arguments.range(0.0, 1.0))
        .setDefault(DEFAULT_THETA).help("similarity threshold");
    parser.addArgument("-l", "--lambda").metavar("lambda").type(Double.class)
        .choices(Arguments.range(0.0, Double.MAX_VALUE)).setDefault(DEFAULT_LAMBDA).help("forgetting factor");
    parser.addArgument("-r", "--report").metavar("period").type(Integer.class).setDefault(DEFAULT_REPORT_PERIOD)
        .help("progress report period");
    parser.addArgument("-i", "--index").type(IndexType.class)
        .choices(IndexType.INV, IndexType.AP, IndexType.L2AP, IndexType.L2)
        .setDefault(IndexType.INV).help("type of indexing");
    parser.addArgument("-f", "--format").type(Format.class).choices(Format.values()).setDefault(Format.BINARY)
        .help("input format");
    parser.addArgument("input").metavar("file")
        .type(Arguments.fileType().verifyExists().verifyIsFile().verifyCanRead()).help("input file");
    Namespace opts = parser.parseArgsOrFail(args);

    final double theta = opts.get("theta");
    final double lambda = opts.get("lambda");
    final int reportPeriod = opts.getInt("report");
    final IndexType idxType = opts.<IndexType>get("index");
    final Format fmt = opts.<Format>get("format");
    final File file = opts.<File>get("input");
    final VectorStream stream = VectorStreamFactory.getVectorStream(file, fmt, new Sequential());
    final long numVectors = stream.numVectors();
    final ProgressTracker tracker = new ProgressTracker(numVectors, reportPeriod);

    final String header = String.format(ALGO + " [d=%s, t=%f, l=%f, i=%s]", file.getName(), theta, lambda,
        idxType.toString());
    System.out.println(header);
    log.info(header);
    final long start = System.currentTimeMillis();
    final IndexStatistics stats = compute(stream, theta, lambda, idxType, tracker);
    final long elapsed = System.currentTimeMillis() - start;
    final String csvLine = Joiner.on(",").join(ALGO, file.getName(), theta, lambda, idxType.toString(), elapsed,
        stats.numPostingEntries(), stats.numCandidates(), stats.numSimilarities(), stats.numMatches());
    System.out.println(csvLine);
    log.info(String.format(ALGO + " [d=%s, t=%f, l=%f, i=%s, time=%d]", file.getName(), theta, lambda,
        idxType.toString(), elapsed));
  }

  public static IndexStatistics compute(Iterable<Vector> stream, double theta, double lambda, IndexType idxType,
      ProgressTracker tracker) {
    final double tau = Commons.tau(theta, lambda);
    System.out.println("Tau = " + tau);
    precomputeFFTable(lambda, 2 * (int) Math.ceil(tau));
    final VectorWindow window = new VectorWindow(tau, idxType.needsMax());

    final Mean avgSize = new Mean();
    long numPostingEntries = 0, numCandidates = 0, numSimilarities = 0;
    int numMatches = 0;
    for (Vector v : stream) {
      if (tracker != null)
        tracker.progress();
      boolean inWindow = window.add(v);
      while (!inWindow) {
        if (window.size() > 0) {
          final IndexStatistics stats = computeBatch(window, theta, lambda, idxType);
          avgSize.increment(stats.size());
          numPostingEntries += stats.numPostingEntries();
          numCandidates += stats.numCandidates();
          numSimilarities += stats.numSimilarities();
          numMatches += stats.numMatches();
        } else {
          window.slide();
        }
        inWindow = window.add(v);
      }
    }
    // last 2 window slides
    while (!window.isEmpty()) {
      final IndexStatistics stats = computeBatch(window, theta, lambda, idxType);
      avgSize.increment(stats.size());
      numPostingEntries += stats.numPostingEntries();
      numCandidates += stats.numCandidates();
      numSimilarities += stats.numSimilarities();
      numMatches += stats.numMatches();
    }
    final StringBuilder sb = new StringBuilder();
    sb.append("Index Statistics:\n");
    sb.append(String.format("Average index size           = %.3f\n", avgSize.getResult()));
    sb.append(String.format("Total number of entries      = %d\n", numPostingEntries));
    sb.append(String.format("Total number of candidates   = %d\n", numCandidates));
    sb.append(String.format("Total number of similarities = %d\n", numSimilarities));
    sb.append(String.format("Total number of matches      = %d", numMatches));
    log.info(sb.toString());
    return new SimpleIndexStatistics((int) avgSize.getResult(), numPostingEntries, numCandidates, numSimilarities,
        numMatches);
  }

  /**
   * Compute the similarity self join on the window.
   * 
   * @param window the vector window
   * @param theta the similarity threshold
   * @param lambda the forgetting factor
   * @param type which type of index to use
   * @return the index statistics
   */
  private static IndexStatistics computeBatch(VectorWindow window, double theta, double lambda, IndexType type) {
    // select and initialize index
    Index index = null;
    switch (type) {
    case INV:
      index = new INVIndex(theta, lambda);
      break;
    case AP:
      index = new APIndex(theta, lambda, window.getMax());
      break;
    case L2AP:
      index = new L2APIndex(theta, lambda, window.getMax());
      break;
    case L2:
      index = new L2Index(theta, lambda);
      break;
    default:
      throw new RuntimeException("Unsupported index type");
    }
    assert (index != null);

    // query and build the index on first half of the buffer
    BatchResult res1 = query(index, window.firstHalf(), true);
    // query the index with the second half of the buffer without indexing it
    BatchResult res2 = query(index, window.secondHalf(), false);
    // slide the window
    window.slide();

    // print results
    for (Entry<Long, Map<Long, Double>> row : res1.rowMap().entrySet()) {
      System.out.println(row.getKey() + " ~ " + formatMap(row.getValue()));
    }
    for (Entry<Long, Map<Long, Double>> row : res2.rowMap().entrySet()) {
      System.out.println(row.getKey() + " ~ " + formatMap(row.getValue()));
    }
    return index.stats();
  }

  private static BatchResult query(Index index, Iterator<Vector> iterator, final boolean addToIndex) {
    final BatchResult result = new BatchResult();
    while (iterator.hasNext()) {
      final Vector v = iterator.next();
      Map<Long, Double> matches = index.queryWith(v, addToIndex);
      for (Entry<Long, Double> e : matches.entrySet()) {
        result.put(v.timestamp(), e.getKey(), e.getValue());
      }
    }
    return result;
  }

  private static class BatchResult extends ForwardingTable<Long, Long, Double> {
    private final Table<Long, Long, Double> delegate = TreeBasedTable.create();

    @Override
    protected Table<Long, Long, Double> delegate() {
      return delegate;
    }
  }
}
