package sssj;

import it.unimi.dsi.fastutil.ints.Int2DoubleAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;

/**
 * A sparse vector in a multidimensional Euclidean space. The vector is identified by a unique timestamp.
 */
public class Vector extends Int2DoubleAVLTreeMap {
  public static final Vector EMPTY_VECTOR = new Vector(Long.MIN_VALUE);

  private long timestamp;

  public Vector() {
    this(0);
  }

  public Vector(long timestamp) {
    this.timestamp = timestamp;
  }

  /**
   * Copy constructor. The values are deep copied.
   * 
   * @param other
   */
  public Vector(Vector other) {
    this.timestamp = other.timestamp;
    this.putAll(other);
  }

  public long timestamp() {
    return timestamp;
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

  //  public static long findSmallestCommonTermID(Vector v1, Vector v2) {
  //    VectorComponent vca1[] = v1.getValue().toVectorComponentArray();
  //    VectorComponent vca2[] = v2.getValue().toVectorComponentArray();
  //    // Arrays.sort(vca1);
  //    // Arrays.sort(vca2);
  //    // empty vectors
  //    if (vca1.length == 0 || vca2.length == 0)
  //      return -1;
  //    int i = vca1.length - 1, j = vca2.length - 1;
  //    while (i >= 0 && j >= 0) {
  //      // FIXME Works only if the dimensions inside the vector are ordered descending
  //      if (vca1[i].getID() > vca2[j].getID()) {
  //        j--;
  //      } else if (vca1[i].getID() < vca2[j].getID()) {
  //        i--;
  //      } else if (vca1[i].getID() == vca2[j].getID()) {
  //        return vca1[i].getID();
  //      }
  //    }
  //    return -1;
  //  }

  /**
   * Computes the dot product of two sparse vectors. The dimensions are expected to be sorted descending.
   * 
   * @param vca1
   *          first vector
   * @param vca2
   *          second vector
   * @param minDim
   *          multiply from max down to this dimension, not included
   * @return the dot product of the vectors as a double
   */
//  public static double dotProduct(VectorComponent[] vca1, VectorComponent[] vca2, long minDim) {
//    // I assume the arrays are already correctly sorted
//    // Arrays.sort(vca1);
//    // Arrays.sort(vca2);
//    // empty vectors
//    if (vca1.length == 0 || vca2.length == 0)
//      return 0;
//    int i = 0, j = 0;
//    double res = 0;
//    while (i < vca1.length && j < vca2.length && vca1[i].getID() > minDim && vca2[j].getID() > minDim) {
//      if (vca1[i].getID() > vca2[j].getID()) {
//        i++;
//      } else if (vca1[i].getID() < vca2[j].getID()) {
//        j++;
//      } else if (vca1[i].getID() == vca2[j].getID()) {
//        res += vca1[i].getWeight() * vca2[j].getWeight();
//        i++;
//        j++;
//      }
//    }
//    return res;
//  }

  /**
   * Updates the vector maxVector to the max of itself and the vector query. 
   * @param maxVector the old max vector
   * @param query the new vector
   * @return the subset of the new vector that was larger than maxVector (for reindexing)
   */
  public static Vector maxByDimension(Vector maxVector, Vector query) {
    Vector result = new Vector();
    for (Int2DoubleMap.Entry e : query.int2DoubleEntrySet()) {
      if (maxVector.get(e.getIntKey()) < e.getDoubleValue()) {
        maxVector.put(e.getIntKey(), e.getDoubleValue());
        result.put(e.getIntKey(), e.getDoubleValue());
      }
    }
    return result;
  }
}
