package sssj.base;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;

public final class CircularBuffer {
  private static final int DEFAULT_SIZE = 1024;
  private int head, tail;
  private int size;
  private ByteBuffer buffer;

  public CircularBuffer() {
    this(DEFAULT_SIZE);
  }

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

  // public LongBuffer put(int index, long l) {
  // // TODO Auto-generated method stub
  // return null;
  // }

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

  // public DoubleBuffer put(int index, double d) {
  // // TODO Auto-generated method stub
  // return null;
  // }

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
    return buffer.capacity() >> 3; // assumes Longs.BYTES = 8
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
