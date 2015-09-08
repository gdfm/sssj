package sssj.base;

import it.unimi.dsi.fastutil.ints.Int2DoubleLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A sparse vector in a multidimensional Euclidean space. The vector is identified by a unique timestamp.
 * Each vector has the following serialization format.
 * VECTOR := VECTOR_ID(long) NUM_ELEMENTS(int) [ELEMENT]+
 * ELEMENT := DIMENSION(int) VALUE(double)
 * Where VECTOR_ID is the vector id (which also represents its timestamp), NUM_ELEMENTS is the number of non-zero-valued dimensions in the vector, and ELEMENT
 * is a single dimension-value pair. Finally, DIMENSION is the id of the dimension and VALUE its value.
 */
// public class Vector extends Int2DoubleAVLTreeMap {
public class Vector extends Int2DoubleLinkedOpenHashMap { // entries are returned in the same order they are added
  public static final Vector EMPTY_VECTOR = new Vector(Long.MIN_VALUE);
  protected long timestamp;
  public double maxValue;

  public Vector() {
    this(0);
  }

  public Vector(long timestamp) {
    this.timestamp = timestamp;
  }

  /**
   * Copy constructor. The values are deep copied.
   * 
   * @param other the vector to copy
   */
  public Vector(Vector other) {
    this.timestamp = other.timestamp;
    this.putAll(other);
  }

  @Override
  public double put(int k, double v) {
    this.maxValue = Math.max(maxValue, v);
    return super.put(k, v);
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
    int result = super.hashCode();
    result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (!(obj instanceof Vector))
      return false;
    Vector other = (Vector) obj;
    if (timestamp != other.timestamp)
      return false;
    return true;
  }

  public double magnitude() {
    double magnitude = 0;
    for (double d : this.values())
      magnitude += d * d;
    magnitude = Math.sqrt(magnitude);
    return magnitude;
  }

  @Override
  public double remove(int k) {
    throw new UnsupportedOperationException("Cannot remove from a vector");
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

  public void read(DataInput in) throws IOException {
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

  /**
   * Updates the vector to the max of itself and the vector query.
   * 
   * @param query
   *          the new vector
   * @return the subset of the new vector that was larger than maxVector (for reindexing)
   */
  public Vector updateMaxByDimension(Vector query) {
    Vector result = new Vector();
    for (Int2DoubleMap.Entry e : query.int2DoubleEntrySet()) {
      if (Double.compare(this.get(e.getIntKey()), e.getDoubleValue()) < 0) {
        this.put(e.getIntKey(), e.getDoubleValue());
        result.put(e.getIntKey(), e.getDoubleValue());
      }
    }
    return result;
  }

  public static double similarity(Vector query, Vector target) {
    double result = 0;
    for (Int2DoubleMap.Entry e : query.int2DoubleEntrySet()) {
      result += e.getDoubleValue() * target.get(e.getIntKey()); // TODO add forgetting factor
    }
    return result;
  }

}
