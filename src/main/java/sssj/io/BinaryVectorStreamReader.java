package sssj.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;

import org.apache.commons.lang.SerializationException;

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

public class BinaryVectorStreamReader implements VectorStream {
  private final ByteBuffer bb;
  private final int numVectors;

  public BinaryVectorStreamReader(File file) throws IOException {
    this(new FileInputStream(file));
  }

  public BinaryVectorStreamReader(FileInputStream input) throws IOException {
    this(input.getChannel().map(MapMode.READ_ONLY, 0L, input.getChannel().size()));
  }

  public BinaryVectorStreamReader(ByteBuffer buffer) { // mostly for testing purposes
    bb = buffer;
    numVectors = bb.getInt();
  }

  @Override
  public int numVectors() {
    return numVectors;
  }

  @Override
  public Iterator<Vector> iterator() {
    return new BinaryVectorIterator();
  }

  public class BinaryVectorIterator implements Iterator<Vector> {
    private final Vector current = new Vector();
    private int numReads;

    @Override
    public boolean hasNext() {
      return numReads < numVectors;
    }

    @Override
    public Vector next() {
      try {
        current.read(bb);
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