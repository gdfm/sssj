package sssj.util;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;

public final class CircularBuffer {
  private static final int DEFAULT_CAPACITY = 16;
  private static final int LOG2_LONG_BYETS = 63 - Long.numberOfLeadingZeros(Longs.BYTES); // log2(Long.BYTES)
  private int head, tail, size;
  private ByteBuffer buffer;

  /**
   * Create a CircularBuffer of default initial size (1024 elements).
   */
  public CircularBuffer() {
    this(DEFAULT_CAPACITY);
  }

  /**
   * Create a CircularBuffer of the specified initial capacity.
   * 
   * @param the initial capacity in number of elements
   */
  public CircularBuffer(int capacity) {
    this.buffer = ByteBuffer.allocate(capacity * Longs.BYTES);
  }

  public long popLong() {
    if (isEmpty())
      throw new BufferUnderflowException();
    size--;
    long l = buffer.getLong(head);
    head = (head + Longs.BYTES) % buffer.capacity();
    if (size() < capacity() / 4) // buffer is at most 25% full
      resize(capacity() / 2); // now it is at most 50% full
    return l;
  }

  public long peekLong(int index) {
    int i = cycleIndex(index);
    return buffer.getLong(i);
  }

  public CircularBuffer pushLong(long l) {
    if (isFull())
      resize(2 * capacity());
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
    if (size() < capacity() / 4) // buffer is at most 25% full
      resize(capacity() / 2); // now it is at most 50% full
    return d;
  }

  public double peekDouble(int index) {
    int i = cycleIndex(index);
    return buffer.getDouble(i);
  }

  public CircularBuffer pushDouble(double d) {
    if (isFull())
      resize(2 * capacity());
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
    if (size() < n)
      throw new BufferUnderflowException();
    size -= n;
    head = (head + n * Longs.BYTES) % buffer.capacity();
    if (size() < capacity() / 4 && capacity() > DEFAULT_CAPACITY) // buffer is at most 25% full
      resize(capacity() / 2); // now it is at most 50% full
  }

  public final boolean isEmpty() {
    return size() == 0;
  }

  public final boolean isFull() {
    return size() == capacity();
  }

  public final int size() {
    return size;
  }

  public final int capacity() {
    return buffer.capacity() >> LOG2_LONG_BYETS;
  }

  private final int cycleIndex(int index) {
    Preconditions.checkArgument(index < size());
    final int idx = (head + index * Longs.BYTES) % buffer.capacity();
    return idx;
  }

  private final void resize(int newCapacity) { // size in number of long elements
    ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity * Longs.BYTES);
    if (size() == 0) { // empty buffer, nothing to do
      assert (head == tail);
    } else if (head < tail) { // straight buffer
      newBuffer.put(buffer.array(), head, tail - head);
    } else { // (head >= tail) wrapped buffer
      newBuffer.put(buffer.array(), head, buffer.capacity() - head);
      newBuffer.put(buffer.array(), 0, tail);
    }
    buffer = newBuffer;
    head = 0;
    tail = (size * Longs.BYTES) % buffer.capacity();
  }
}
