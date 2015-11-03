package sssj.io;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import sssj.io.Format;
import sssj.io.VectorStreamReader;

public class VectorStreamReaderTest {
  public static final String EXAMPLE_FILENAME = "/example.txt";

  @Test
  public void test() throws IOException {
    File file = new File(this.getClass().getResource(EXAMPLE_FILENAME).getPath());
    VectorStreamReader stream = new VectorStreamReader(file, Format.SSSJ);
    int[] sizes = new int[] { 8, 14, 14, 8, 9, 10, 12, 18, 10 };
    int i = 0;
    for (Vector v : stream)
      assertEquals(sizes[i++], v.size());
  }
}
