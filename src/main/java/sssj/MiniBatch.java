package sssj;

import static sssj.base.Commons.*;
import static sssj.base.Commons.IndexType.INVERTED;

import java.io.BufferedReader;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import sssj.base.Commons;
import sssj.base.Commons.BatchResult;
import sssj.base.Commons.IndexType;
import sssj.base.Vector;
import sssj.base.VectorBuffer;
import sssj.index.APIndex;
import sssj.index.Index;
import sssj.index.InvertedIndex;
import sssj.index.L2APIndex;
import sssj.index.StreamingIndex;
import sssj.io.Format;
import sssj.io.VectorStreamReader;
import sssj.time.Timeline.Sequential;

import com.github.gdfm.shobaidogu.IOUtils;
import com.github.gdfm.shobaidogu.ProgressTracker;

/**
 * MiniBatch micro-batch method. Keeps a buffer of vectors of length 2*tau. When the buffer is full, index and query the first half of the vectors with a batch
 * index (Inverted, AP, L2AP), and query the index built so far with the second half of the buffer. Discard the first half of the buffer, retain the second half
 * as the new first half, and repeat the process.
 */
public class MiniBatch {
  private static final Logger log = LoggerFactory.getLogger(MiniBatch.class);

  public static void main(String[] args) throws Exception {
    ArgumentParser parser = ArgumentParsers.newArgumentParser("MiniBatch").description("SSSJ in MiniBatch mode.")
        .defaultHelp(true);
    parser.addArgument("-t", "--theta").metavar("theta").type(Double.class).choices(Arguments.range(0.0, 1.0))
        .setDefault(DEFAULT_THETA).help("similarity threshold");
    parser.addArgument("-l", "--lambda").metavar("lambda").type(Double.class)
        .choices(Arguments.range(0.0, Double.MAX_VALUE)).setDefault(DEFAULT_LAMBDA).help("forgetting factor");
    parser.addArgument("-r", "--report").metavar("period").type(Integer.class).setDefault(DEFAULT_REPORT_PERIOD)
        .help("progress report period");
    parser.addArgument("-i", "--index").type(IndexType.class).choices(IndexType.values()).setDefault(INVERTED)
        .help("type of indexing");
    parser.addArgument("-f", "--format").type(Format.class).choices(Format.values()).setDefault(Format.SSSJ)
        .help("input format");
    parser.addArgument("input").metavar("file")
        .type(Arguments.fileType().verifyExists().verifyIsFile().verifyCanRead()).help("input file");
    Namespace res = parser.parseArgsOrFail(args);

    final double theta = res.get("theta");
    final double lambda = res.get("lambda");
    final int reportPeriod = res.getInt("report");
    final IndexType idxType = res.<IndexType>get("index");
    final Format fmt = res.<Format>get("format");
    final String filename = res.getString("input");
    final BufferedReader reader = IOUtils.getBufferedReader(filename);
    final int numItems = IOUtils.getNumberOfLines(IOUtils.getBufferedReader(filename));
    final ProgressTracker tracker = new ProgressTracker(numItems, reportPeriod);
    final VectorStreamReader stream = new VectorStreamReader(reader, fmt, new Sequential());

    System.out.println(String.format("MiniBatch [t=%f, l=%f, i=%s]", theta, lambda, idxType.toString()));
    log.info(String.format("MiniBatch [t=%f, l=%f, i=%s]", theta, lambda, idxType.toString()));
    compute(stream, theta, lambda, idxType, tracker);
  }

  public static void compute(Iterable<Vector> stream, double theta, double lambda, IndexType idxType,
      ProgressTracker tracker) {
    final double tau = Commons.tau(theta, lambda);
    System.out.println("Tau = " + tau);
    VectorBuffer window = new VectorBuffer(tau);

    for (Vector v : stream) {
      if (tracker != null)
        tracker.progress();
      boolean inWindow = window.add(v);
      while (!inWindow) {
        if (window.size() > 0)
          computeResults(window, theta, lambda, idxType);
        else
          window.slide();
        inWindow = window.add(v);
      }
    }
    // last 2 window slides
    while (!window.isEmpty())
      computeResults(window, theta, lambda, idxType);
  }

  private static void computeResults(VectorBuffer window, double theta, double lambda, IndexType type) {
    // select and initialize index
    Index index = null;
    switch (type) {
    case INVERTED:
      index = new InvertedIndex(theta, lambda);
      break;
    case ALL_PAIRS:
      index = new APIndex(theta, lambda, window.getMax());
      break;
    case L2AP:
      index = new L2APIndex(theta, lambda, window.getMax());
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
      System.out.println(row.getKey() + ": " + formatMap(row.getValue()));
    }
    for (Entry<Long, Map<Long, Double>> row : res2.rowMap().entrySet()) {
      System.out.println(row.getKey() + ": " + formatMap(row.getValue()));
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
