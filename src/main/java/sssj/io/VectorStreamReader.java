package sssj.io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import sssj.Vector;

import com.github.gdfm.shobaidogu.LineIterable;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

public class VectorStreamReader implements Iterable<Vector> {
  private LineIterable it;

  public VectorStreamReader(BufferedReader reader) throws FileNotFoundException, IOException {
    it = new LineIterable(reader);
  }

  public Iterator<Vector> iterator() {
    return Iterators.transform(it.iterator(), new Function<String, Vector>() {
      public Vector apply(String input) {
        String[] tokens = input.split(" ");
        Long ts = Long.parseLong(tokens[0]);
        Vector result = new Vector(ts);
        for (int i = 1; i < tokens.length; i += 2) {
          int key = Integer.parseInt(tokens[i]);
          double val = Double.parseDouble(tokens[i + 1]);
          result.put(key, val);
        }
        return Vector.l2normalize(result);
      }
    });
  }
}
