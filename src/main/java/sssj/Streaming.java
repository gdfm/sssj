package sssj;

import static sssj.base.Commons.*;

import java.io.File;
import java.util.Map;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sssj.base.Commons.IndexType;
import sssj.base.Vector;
import sssj.index.Index;
import sssj.index.StreamingInvertedIndex;
import sssj.index.StreamingL2APIndex;
import sssj.io.Format;
import sssj.io.VectorStream;
import sssj.io.VectorStreamFactory;
import sssj.time.Timeline.Sequential;

import com.github.gdfm.shobaidogu.ProgressTracker;

public class Streaming {
  private static final Logger log = LoggerFactory.getLogger(Streaming.class);
  private static final String ALGO = "Streaming";

  public static void main(String[] args) throws Exception {
    ArgumentParser parser = ArgumentParsers.newArgumentParser(ALGO).description("SSSJ in " + ALGO + " mode.")
        .defaultHelp(true);
    parser.addArgument("-t", "--theta").metavar("theta").type(Double.class).choices(Arguments.range(0.0, 1.0))
        .setDefault(DEFAULT_THETA).help("similarity threshold");
    parser.addArgument("-l", "--lambda").metavar("lambda").type(Double.class)
        .choices(Arguments.range(0.0, Double.MAX_VALUE)).setDefault(DEFAULT_LAMBDA).help("forgetting factor");
    parser.addArgument("-r", "--report").metavar("period").type(Integer.class).setDefault(DEFAULT_REPORT_PERIOD)
        .help("progress report period");
    parser.addArgument("-i", "--index").type(IndexType.class).choices(IndexType.INVERTED, IndexType.L2AP)
        .setDefault(IndexType.INVERTED).help("type of indexing");
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
    final int numVectors = stream.numVectors();
    final ProgressTracker tracker = new ProgressTracker(numVectors, reportPeriod);

    System.out.println(String.format(ALGO + " [t=%f, l=%f, i=%s]", theta, lambda, idxType.toString()));
    log.info(String.format(ALGO + " [t=%f, l=%f, i=%s]", theta, lambda, idxType.toString()));
    long start = System.currentTimeMillis();
    compute(stream, theta, lambda, idxType, tracker);
    long elapsed = System.currentTimeMillis() - start;
    System.out.println(String.format(ALGO + "-%s, %f, %f, %d", idxType.toString(), theta, lambda, elapsed));
    log.info(String.format(ALGO + " [t=%f, l=%f, i=%s, time=%d]", theta, lambda, idxType.toString(), elapsed));
  }

  public static void compute(Iterable<Vector> stream, double theta, double lambda, IndexType type,
      ProgressTracker tracker) {
    Index index = null;
    switch (type) {
    case INVERTED:
      index = new StreamingInvertedIndex(theta, lambda);
      break;
    case L2AP:
      index = new StreamingL2APIndex(theta, lambda);
      break;
    default:
      throw new RuntimeException("Unsupported index type");
    }
    assert (index != null);

    Mean avgSize = new Mean(), avgMaxLength = new Mean();
    for (Vector v : stream) {
      if (tracker != null)
        tracker.progress();
      Map<Long, Double> results = index.queryWith(v, true);
      avgSize.increment(index.size());
      avgMaxLength.increment(index.maxLength());
      if (!results.isEmpty())
        System.out.println(v.timestamp() + " ~ " + formatMap(results));
    }
    final String statsString = String.format("Avg. index size = %.3f, Avg. max posting list length = %.3f",
        avgSize.getResult(), avgMaxLength.getResult());
    log.info(statsString);
    System.out.println(statsString);
  }
}
