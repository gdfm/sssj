package sssj;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;

import org.junit.Test;

import sssj.Vector;
import sssj.io.Format;
import sssj.io.VectorStreamReader;

import com.github.gdfm.shobaidogu.IOUtils;

public class StreamReaderTest {
  public static final String EXAMPLE_FILENAME = "/example.txt";

  @Test
  public void test() throws IOException {
    BufferedReader reader = IOUtils.getBufferedReader(EXAMPLE_FILENAME);
    VectorStreamReader stream = new VectorStreamReader(reader, Format.SSSJ);
    int[] sizes = new int[] { 8, 14, 14, 8, 9, 10, 12, 18, 10 };
    int i = 0;
    for (Vector v : stream)
      assertEquals(sizes[i++], v.size());
  }
}
