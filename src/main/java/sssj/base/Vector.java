package sssj.base;

import it.unimi.dsi.fastutil.ints.Int2DoubleLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleSortedMap.FastSortedEntrySet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A sparse vector in a multidimensional Euclidean space. The vector is identified by a unique timestamp.
 * Each vector has the following serialization format.
 * VECTOR := VECTOR_ID(long) NUM_ELEMENTS(int) [ELEMENT]+
 * ELEMENT := DIMENSION(int) VALUE(double)
 * Where VECTOR_ID is the vector id (which also represents its timestamp), NUM_ELEMENTS is the number of non-zero-valued dimensions in the vector, and ELEMENT
 * is a single dimension-value pair. Finally, DIMENSION is the id of the dimension and VALUE its value.
 */
// public class Vector extends Int2DoubleAVLTreeMap {
public class Vector { // entries are returned in the same order they are added
  public static final Vector EMPTY_VECTOR = new Vector(Long.MIN_VALUE);
  protected Int2DoubleLinkedOpenHashMap data = new Int2DoubleLinkedOpenHashMap();
  protected long timestamp;
  protected double maxValue;

  public Vector() {
    this(0);
  }

  public Vector(long timestamp) {
    this.timestamp = timestamp;
    this.maxValue = 0;
  }

  /**
   * Copy constructor. The values are deep copied.
   * 
   * @param other the vector to copy
   */
  public Vector(Vector other) {
    this.timestamp = other.timestamp;
    this.maxValue = other.maxValue;
    data.putAll(other.data);
  }

  public double put(int k, double v) {
    this.maxValue = Math.max(maxValue, v);
    return data.put(k, v);
  }

  public double get(int k) {
    return data.get(k);
  }

  public FastSortedEntrySet int2DoubleEntrySet() {
    return data.int2DoubleEntrySet();
  }

  public int size() {
    return data.size();
  }

  public double maxValue() {
    return maxValue;
  }

  public long timestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return timestamp + "\t" + super.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((data == null) ? 0 : data.hashCode());
    result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof Vector))
      return false;
    Vector other = (Vector) obj;
    if (data == null) {
      if (other.data != null)
        return false;
    } else if (!data.equals(other.data))
      return false;
    if (timestamp != other.timestamp)
      return false;
    return true;
  }

  public double magnitude() {
    double magnitude = 0;
    for (double d : data.values())
      magnitude += d * d;
    magnitude = Math.sqrt(magnitude);
    return magnitude;
  }

  public void read(ByteBuffer in) throws IOException {
    data.clear();
    this.setTimestamp(in.getLong());
    int numElements = in.getInt();
    for (int i = 0; i < numElements; i++) {
      int dim = in.getInt();
      double val = in.getDouble();
      this.put(dim, val);
    }
  }

  public void write(ByteBuffer out) throws IOException {
    out.putLong(this.timestamp());
    out.putInt(this.size());
    for (Int2DoubleMap.Entry e : this.int2DoubleEntrySet()) {
      out.putInt(e.getIntKey());
      out.putDouble(e.getDoubleValue());
    }
  }

  public void read(DataInput in) throws IOException {
    data.clear();
    this.setTimestamp(in.readLong());
    int numElements = in.readInt();
    for (int i = 0; i < numElements; i++) {
      int dim = in.readInt();
      double val = in.readDouble();
      this.put(dim, val);
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeLong(this.timestamp());
    out.writeInt(this.size());
    for (Int2DoubleMap.Entry e : this.int2DoubleEntrySet()) {
      out.writeInt(e.getIntKey());
      out.writeDouble(e.getDoubleValue());
    }
  }

  public static Vector l2normalize(Vector v) {
    Vector result;
    double magnitude = v.magnitude();
    if (Double.compare(1.0, magnitude) != 0) {
      result = new Vector(v.timestamp());
      for (Int2DoubleMap.Entry e : v.int2DoubleEntrySet())
        result.put(e.getIntKey(), e.getDoubleValue() / magnitude);
    } else {
      result = v;
    }
    return result;
  }

  public static double similarity(Vector query, Vector target) {
    double result = 0;
    for (Int2DoubleMap.Entry e : query.int2DoubleEntrySet()) {
      result += e.getDoubleValue() * target.get(e.getIntKey());
    }
    return result;
  }
}
