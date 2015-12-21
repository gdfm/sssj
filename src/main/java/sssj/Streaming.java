package sssj;

import static sssj.util.Commons.*;

import java.io.File;
import java.util.Map;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sssj.index.Index;
import sssj.index.IndexStatistics;
import sssj.index.IndexType;
import sssj.index.streaming.StreamingINVIndex;
import sssj.index.streaming.StreamingL2APIndex;
import sssj.index.streaming.StreamingL2Index;
import sssj.io.Format;
import sssj.io.Vector;
import sssj.io.VectorStream;
import sssj.io.VectorStreamFactory;
import sssj.time.Timeline.Sequential;

import com.github.gdfm.shobaidogu.ProgressTracker;
import com.google.common.base.Joiner;

/**
 * Streaming method. Fully incremental, online (zero latency). Keeps the index pruned via time filtering.
 * Efficient pruning supported via circular buffers. Supports three types of index (INV, L2AP, L2).
 */
public class Streaming {
  private static final String ALGO = "Streaming";
  private static final Logger log = LoggerFactory.getLogger(Streaming.class);

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
        .choices(IndexType.INV, IndexType.L2AP, IndexType.L2).setDefault(IndexType.INV)
        .help("type of indexing");
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

  public static IndexStatistics compute(Iterable<Vector> stream, double theta, double lambda, IndexType type,
      ProgressTracker tracker) {
    Index index = null;
    switch (type) {
    case INV:
      index = new StreamingINVIndex(theta, lambda);
      break;
    case L2AP:
      index = new StreamingL2APIndex(theta, lambda);
      break;
    case L2:
      index = new StreamingL2Index(theta, lambda);
      break;
    default:
      throw new RuntimeException("Unsupported index type");
    }
    assert (index != null);

    Mean avgSize = new Mean();
    for (Vector v : stream) {
      if (tracker != null)
        tracker.progress();
      Map<Long, Double> results = index.queryWith(v, true);
      IndexStatistics stats = index.stats();
      avgSize.increment(stats.size());
      if (!results.isEmpty())
        System.out.println(v.timestamp() + " ~ " + formatMap(results));
    }
    final StringBuilder sb = new StringBuilder();
    sb.append("Index Statistics:\n");
    sb.append(String.format("Average index size           = %.3f\n", avgSize.getResult()));
    sb.append(String.format("Total number of entries      = %d\n", index.stats().numPostingEntries()));
    sb.append(String.format("Total number of candidates   = %d\n", index.stats().numCandidates()));
    sb.append(String.format("Total number of similarities = %d\n", index.stats().numSimilarities()));
    sb.append(String.format("Total number of matches      = %d", index.stats().numMatches()));
    log.info(sb.toString());
    return index.stats();
  }
}
