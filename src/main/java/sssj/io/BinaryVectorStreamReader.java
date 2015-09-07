package sssj.io;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.commons.lang.SerializationException;

import sssj.base.Vector;

/**
 * A Vector reader for binary serialized data. The binary format is described next.
 * FILE := NUM_VECTORS(int) [VECTOR]+
 *
 * Here, NUM_VECTORS is the number of vectors in the input, and VECTOR is the serialization of a single {@link Vector}.
 * For performance reasons, the Vector returned by the reader is reused, so you need to copy it if you want to save it.
 * The class also assumes the values are already l2 normalized.
 * 
 * @throws IOException
 */

public class BinaryVectorStreamReader implements Iterable<Vector> {
  private final DataInputStream dis;

  public BinaryVectorStreamReader(InputStream input) throws IOException {
    dis = new DataInputStream(input);
  }

  @Override
  public Iterator<Vector> iterator() {
    try {
      return new BinaryVectorIterator();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public class BinaryVectorIterator implements Iterator<Vector> {
    private final Vector current = null;
    private final int numVectors;
    private int numReads;

    public BinaryVectorIterator() throws IOException {
      numVectors = dis.readInt();
    }

    @Override
    public boolean hasNext() {
      return numReads < numVectors;
    }

    @Override
    public Vector next() {
      current.clear();
      try {
        current.read(dis);
      } catch (IOException e) {
        e.printStackTrace();
        throw new SerializationException();
      }
      numReads++;
      return current;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}

// FileChannel fc2 = new FileInputStream(tmp).getChannel();
// bb.clear();
// while (fc2.read(bb) > 0) {
// while (bb.remaining() > 0)
// total += bb.get();
// bb.clear();
// }
// fc2.close();