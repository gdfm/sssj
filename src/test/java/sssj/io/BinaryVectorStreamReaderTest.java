package sssj.io;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.junit.Test;

import sssj.base.Vector;

public class BinaryVectorStreamReaderTest {

  @Test
  public void test() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    out.writeInt(1); // writing one vector
    Vector v = new Vector(111);
    v.put(1, 1.0);
    v.put(2, 2.0);
    v.put(3, 3.0);
    v.write(out);
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    BinaryVectorStreamReader reader = new BinaryVectorStreamReader(in);
    Iterator<Vector> it = reader.iterator();
    assertTrue(it.hasNext());
    Vector r = it.next();
    assertFalse(it.hasNext()); // only one vector
    assertEquals(v, r); // recovered v
  }
}
