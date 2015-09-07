package sssj;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sssj.base.Vector;
import sssj.io.Format;
import sssj.io.VectorStream;
import sssj.io.VectorStreamReader;
import sssj.time.Timeline;

import com.google.common.base.Preconditions;

public class BinaryConverter {
  private static final Logger log = LoggerFactory.getLogger(BinaryConverter.class);

  public static void main(String[] args) throws Exception {
    ArgumentParser parser = ArgumentParsers.newArgumentParser("Convert")
        .description("Convert files to binary SSSJ format.").defaultHelp(true);
    parser.addArgument("-f", "--format").type(Format.class).choices(Format.values()).setDefault(Format.SSSJ)
        .help("input format");
    parser.addArgument("-t", "--timeline").choices("sequential", "poisson").help("timeline to apply");
    parser.addArgument("-r", "--rate").type(Double.class).setDefault(1.0).help("rate for the Poisson timeline");
    parser.addArgument("input").metavar("input")
        .type(Arguments.fileType().verifyExists().verifyIsFile().verifyCanRead()).help("input file");
    parser.addArgument("output").metavar("output").type(Arguments.fileType().verifyCanCreate()).help("output file");
    Namespace opts = parser.parseArgsOrFail(args);

    final double rate = opts.getDouble("rate");
    final Format fmt = opts.<Format>get("format");
    final String tmls = opts.get("timeline");
    final Timeline tml;
    if ("sequential".equalsIgnoreCase(tmls)) {
      tml = new Timeline.Sequential();
    } else if ("poisson".equalsIgnoreCase(tmls)) {
      tml = new Timeline.Poisson(rate);
    } else {
      tml = null;
    }
    Preconditions.checkArgument(tml != null || fmt == Format.SSSJ,
        "Please specify a timeline or an input format with timestamp information. Timeline=%s, Format=%s.", tml, fmt);

    final File file = opts.<File>get("input");
    final VectorStream stream = new VectorStreamReader(file, fmt, tml);
    final int numVectors = stream.numVectors();
    final DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(
        opts.<File>get("output"))));

    log.info("Converting input file {} in format {} with {} vectors to binary output file {} with timeline {}",
        opts.getString("input"), fmt, numVectors, opts.getString("output"), tml);
    dos.writeInt(numVectors);
    for (Vector v : stream) {
      v.write(dos);
    }
    dos.close();
  }
}
