package sssj;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import sssj.base.Vector;
import sssj.io.Format;
import sssj.io.VectorStreamReader;
import sssj.time.Timeline;

import com.github.gdfm.shobaidogu.IOUtils;

public class BinaryConverter {
  public static void main(String[] args) throws Exception {
    ArgumentParser parser = ArgumentParsers.newArgumentParser("Converter")
        .description("Convert files to binary SSSJ format.").defaultHelp(true);
    parser.addArgument("-f", "--format").type(Format.class).choices(Format.values()).setDefault(Format.SSSJ)
        .help("input format");
    parser.addArgument("-t", "--timeline").type(Timeline.class).choices(Timeline.TIMELINES).help("timeline to apply");
    parser.addArgument("input").metavar("input")
        .type(Arguments.fileType().verifyExists().verifyIsFile().verifyCanRead()).help("input file");
    parser.addArgument("output").metavar("output").type(Arguments.fileType().verifyCanCreate().verifyCanWrite())
        .help("output file");
    Namespace opts = parser.parseArgsOrFail(args);

    final Format fmt = opts.<Format>get("format");
    final Timeline tml = opts.<Timeline>get("timeline");
    final BufferedReader reader = new BufferedReader(new FileReader(opts.<File>get("input")));
    final VectorStreamReader stream = new VectorStreamReader(reader, fmt, tml);
    final DataOutputStream dos = new DataOutputStream(new FileOutputStream(opts.<File>get("output")));
    final int numItems = IOUtils.getNumberOfLines(IOUtils.getBufferedReader(opts.getString("input")));

    dos.writeInt(numItems);
    for (Vector v : stream) {
      v.write(dos);
    }
    dos.close();
  }
}
