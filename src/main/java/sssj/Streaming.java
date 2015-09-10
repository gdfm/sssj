package sssj;

import static sssj.base.Commons.*;

import java.io.File;
import java.util.Map;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sssj.base.Commons.IndexType;
import sssj.base.Vector;
import sssj.index.Index;
import sssj.index.StreamingIndex;
import sssj.io.Format;
import sssj.io.VectorStream;
import sssj.io.VectorStreamFactory;
import sssj.time.Timeline.Sequential;

import com.github.gdfm.shobaidogu.ProgressTracker;

public class Streaming {
  private static final Logger log = LoggerFactory.getLogger(Streaming.class);

  public static void main(String[] args) throws Exception {
    ArgumentParser parser = ArgumentParsers.newArgumentParser("Streaming").description("SSSJ in Streaming mode.")
        .defaultHelp(true);
    parser.addArgument("-t", "--theta").metavar("theta").type(Double.class).choices(Arguments.range(0.0, 1.0))
        .setDefault(DEFAULT_THETA).help("similarity threshold");
    parser.addArgument("-l", "--lambda").metavar("lambda").type(Double.class)
        .choices(Arguments.range(0.0, Double.MAX_VALUE)).setDefault(DEFAULT_LAMBDA).help("forgetting factor");
    parser.addArgument("-r", "--report").metavar("period").type(Integer.class).setDefault(DEFAULT_REPORT_PERIOD)
        .help("progress report period");
    parser.addArgument("-i", "--index").type(IndexType.class).choices(IndexType.values())
        .setDefault(IndexType.INVERTED).help("type of indexing");
    parser.addArgument("-f", "--format").type(Format.class).choices(Format.values()).setDefault(Format.SSSJ)
        .help("input format");
    parser.addArgument("input").metavar("file")
        .type(Arguments.fileType().verifyExists().verifyIsFile().verifyCanRead()).help("input file");
    Namespace opts = parser.parseArgsOrFail(args);

    final double theta = opts.get("theta");
    final double lambda = opts.get("lambda");
    final int reportPeriod = opts.getInt("report");
    // final IndexType idxType = res.<IndexType>get("index");
    final Format fmt = opts.<Format>get("format");
    final File file = opts.<File>get("input");
    final VectorStream stream = VectorStreamFactory.getVectorStream(file, fmt, new Sequential());
    final int numVectors = stream.numVectors();
    final ProgressTracker tracker = new ProgressTracker(numVectors, reportPeriod);

    System.out.println(String.format("Streaming [t=%f, l=%f]", theta, lambda));
    log.info(String.format("Streaming [t=%f, l=%f]", theta, lambda));
    long start = System.currentTimeMillis();
    compute(stream, theta, lambda, tracker);
    long elapsed = System.currentTimeMillis() - start;
    System.out.println(String.format("Streaming, %f, %f, %d", theta, lambda, elapsed));
    log.info(String.format("Streaming [t=%f, l=%f, time=%d]", theta, lambda, elapsed));
  }

  public static void compute(Iterable<Vector> stream, double theta, double lambda, ProgressTracker tracker) {
    Index index = new StreamingIndex(theta, lambda);
    // TODO first update MAX, then query, then index
    for (Vector v : stream) {
      if (tracker != null)
        tracker.progress();

      Map<Long, Double> results = index.queryWith(v);
      if (!results.isEmpty())
        System.out.println(v.timestamp() + " ~ " + formatMap(results));

      index.addVector(v);
    }
  }
}
