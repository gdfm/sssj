package sssj.io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import sssj.base.Vector;
import sssj.time.Timeline;
import sssj.time.TimeStamper;

import com.github.gdfm.shobaidogu.LineIterable;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

public class VectorStreamReader implements Iterable<Vector> {
  private final LineIterable it;
  private final Format format;
  private final TimeStamper ts;

  public VectorStreamReader(BufferedReader reader, Format format) throws FileNotFoundException, IOException {
    this(reader, format, null);
    Preconditions.checkArgument(format == Format.SSSJ); // the format needs to have a timestamp
  }

  public VectorStreamReader(BufferedReader reader, Format format, Timeline timeline) throws FileNotFoundException,
      IOException {
    this.it = new LineIterable(reader);
    this.format = format;
    this.ts = timeline != null ? new TimeStamper(timeline) : null;
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
