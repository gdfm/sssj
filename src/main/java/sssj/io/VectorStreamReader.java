package sssj.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import sssj.time.TimeStamper;
import sssj.time.Timeline;

import com.github.gdfm.shobaidogu.IOUtils;
import com.github.gdfm.shobaidogu.LineIterable;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

public class VectorStreamReader implements VectorStream {
  private final LineIterable it;
  private final Format format;
  private final TimeStamper ts;
  private final int numVectors;

  public VectorStreamReader(File file, Format format) throws FileNotFoundException, IOException {
    this(file, format, null);
    Preconditions.checkArgument(format == Format.SSSJ); // the format needs to have a timestamp
  }

  public VectorStreamReader(File file, Format format, Timeline timeline) throws FileNotFoundException, IOException {
    this.numVectors = IOUtils.getNumberOfLines(new FileReader(file));
    this.it = new LineIterable(file);
    this.format = format;
    Preconditions.checkArgument(timeline != null || format == Format.SSSJ,
        "Specify a timeline or an input format with timestamp information. Timeline=%s, Format=%s.", timeline, format);
    this.ts = timeline != null ? new TimeStamper(timeline) : null;
  }

  @Override
  public long numVectors() {
    return numVectors;
  }

  @Override
  public Iterator<Vector> iterator() {
    Iterator<Vector> result = Iterators.transform(it.iterator(), format.getRecordParser()); // parser
    result = Iterators.transform(result, new Function<Vector, Vector>() { // decorator normalizer
          @Override
          public Vector apply(Vector input) {
            return Vector.l2normalize(input);
          }
        });
    if (ts != null)
      result = Iterators.transform(result, ts); // decorator timestamper
    return result;
  }
}
