package sssj;

import java.io.BufferedReader;
import java.io.IOException;

import org.junit.Test;

import sssj.Vector;
import sssj.io.VectorStreamReader;

import com.github.gdfm.shobaidogu.IOUtils;

public class StreamReaderTest {
  public static final String EXAMPLE_FILENAME = "/example.txt";

  @Test
  public void test() throws IOException {
    BufferedReader reader = IOUtils.getBufferedReader(EXAMPLE_FILENAME);
    VectorStreamReader stream = new VectorStreamReader(reader);
    for (Vector v : stream)
      System.out.println(v);
  }
}
