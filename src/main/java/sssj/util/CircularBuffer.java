package sssj.util;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;

public final class CircularBuffer {
  private static final int DEFAULT_SIZE = 1024;
  private static final int LOG2_LONG_BYETS = 63 - Long.numberOfLeadingZeros(Longs.BYTES); // log2(Long.BYTES)
  private int head, tail, size;
  private ByteBuffer buffer;

  /**
   * Create a CircularBuffer of default initial size (1024 elements).
   */
  public CircularBuffer() {
    this(DEFAULT_SIZE);
  }

  /**
   * Create a CircularBuffer of the specified initial size.
   * 
   * @param the initial number of elements to hold in the circular buffer
   */
  public CircularBuffer(int size) {
    this.buffer = ByteBuffer.allocate(size * Longs.BYTES);
  }

  public long popLong() {
    if (isEmpty())
      throw new BufferUnderflowException();
    size--;
    long l = buffer.getLong(head);
    head = (head + Longs.BYTES) % buffer.capacity();
    return l;
  }

  public long peekLong(int index) {
    int i = cycleIndex(index);
    return buffer.getLong(i);
  }

  public CircularBuffer pushLong(long l) {
    if (isFull())
      grow(2 * buffer.capacity());
    size++;
    buffer.putLong(tail, l);
    tail = (tail + Longs.BYTES) % buffer.capacity();
    return this;
  }

  public double popDouble() {
    if (isEmpty())
      throw new BufferUnderflowException();
    size--;
    double d = buffer.getDouble(head);
    head = (head + Longs.BYTES) % buffer.capacity();
    return d;
  }

  public double peekDouble(int index) {
    int i = cycleIndex(index);
    return buffer.getDouble(i);
  }

  public CircularBuffer pushDouble(double d) {
    if (isFull())
      grow(2 * buffer.capacity());
    size++;
    buffer.putDouble(tail, d);
    tail = (tail + Longs.BYTES) % buffer.capacity();
    return this;
  }

  /**
   * Drop elements from the front (head) of the buffer.
   * 
   * @param n how many elements to drop.
   */
  public void trimHead(int n) {
    if (this.size() < n)
      throw new BufferUnderflowException();
    size -= n;
    head = (head + n * Longs.BYTES) % buffer.capacity();
  }

  public final boolean isEmpty() {
    return size == 0;
  }

  public final boolean isFull() {
    return size * Longs.BYTES == buffer.capacity();
  }

  public final int size() {
    return size;
  }

  public final int capacity() {
    return buffer.capacity() >> LOG2_LONG_BYETS;
  }

  private final int cycleIndex(int index) {
    Preconditions.checkArgument(index < size());
    int i = (head + index * Longs.BYTES) % buffer.capacity();
    return i;
  }

  private final void grow(int newSize) {
    ByteBuffer newBuffer = ByteBuffer.allocate(newSize);
    if (tail > head)
      newBuffer.put(buffer.array(), head, tail - head);
    else {
      newBuffer.put(buffer.array(), head, buffer.capacity() - head);
      newBuffer.put(buffer.array(), 0, tail);
    }
    buffer = newBuffer;
    head = 0;
    tail = (size * Longs.BYTES) % buffer.capacity();
  }
}
