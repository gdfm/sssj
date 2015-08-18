package sssj.io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import sssj.Vector;
import sssj.io.ParserFactory.Format;

import com.github.gdfm.shobaidogu.LineIterable;
import com.google.common.collect.Iterators;

public class VectorStreamReader implements Iterable<Vector> {
  private LineIterable it;
  protected final Format format;

  public VectorStreamReader(BufferedReader reader, Format format) throws FileNotFoundException, IOException {
    this.it = new LineIterable(reader);
    this.format = format;
  }

  public Iterator<Vector> iterator() {
    return Iterators.transform(it.iterator(), format.getRecordParser());
  }
}
