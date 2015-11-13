package sssj.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import org.apache.commons.lang.SerializationException;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;

/**
 * A Vector reader for binary serialized data. The binary format is described next.
 * FILE := NUM_VECTORS(long) [VECTOR]+
 *
 * Here, NUM_VECTORS is the number of vectors in the input, and VECTOR is the serialization of a single {@link Vector}.
 * For performance reasons, the Vector returned by the reader is reused, so you need to copy it if you want to save it.
 * The class also assumes the values are already l2 normalized.
 * 
 * @throws IOException
 */

public class BinaryVectorStreamReader implements VectorStream {
  private final ByteBuffer bb;
  private final FileChannel channel;
  private final long numVectors;

  public BinaryVectorStreamReader(File file) throws IOException {
    this(new FileInputStream(file));
  }

  public BinaryVectorStreamReader(FileInputStream input) throws IOException {
    channel = input.getChannel();
    bb = ByteBuffer.allocateDirect(10 * 1024 * 1024); // buffer needs to be large enough for a single vector
    channel.read(bb);
    bb.flip();
    numVectors = bb.getLong();
  }

  BinaryVectorStreamReader(ByteBuffer buffer) { // for testing purposes
    channel = null;
    bb = buffer;
    numVectors = bb.getLong();
  }

  private int refill() throws IOException {
    bb.compact();
    final int nread = channel.read(bb);
    bb.flip();
    return nread;
  }

  @Override
  public long numVectors() {
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
        // refill the ByteBuffer if not enough bytes left to read
        if (bb.remaining() < Ints.BYTES)
          if (refill() < Ints.BYTES)
            throw new IllegalStateException("Could not read from input file");
        // peek the size of the vector
        bb.mark();
        // vector size in bytes = int + long + size * (int + double)
        final int vectorBytes = (bb.getInt() + 1) * (Ints.BYTES + Doubles.BYTES);
        bb.reset();
        if (bb.remaining() < vectorBytes)
          if (refill() < vectorBytes)
            throw new IllegalStateException("Internal vector buffer too small");
        // now we are sure that the ByteBuffer contains the whole next vector
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