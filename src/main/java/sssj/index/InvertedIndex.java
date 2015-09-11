package sssj.index;

import static sssj.base.Commons.forgetFactor;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.Iterator;
import java.util.Map;

import sssj.base.Commons;
import sssj.base.Vector;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;

public class InvertedIndex implements Index {
  private Int2ReferenceMap<PostingList> idx = new Int2ReferenceOpenHashMap<>();
  private int size = 0;
  private final double theta;
  private final double lambda;

  public InvertedIndex(double theta, double lambda) {
    this.theta = theta;
    this.lambda = lambda;
  }

  @Override
  public Map<Long, Double> queryWith(final Vector v) {
    Long2DoubleOpenHashMap accumulator = new Long2DoubleOpenHashMap(size);
    for (Entry e : v.int2DoubleEntrySet()) {
      int dimension = e.getIntKey();
      if (!idx.containsKey(dimension))
        continue;
      PostingList list = idx.get(dimension);
      double queryWeight = e.getDoubleValue();
      for (PostingEntry pe : list) {
        long targetID = pe.getLongKey();
        double targetWeight = pe.getDoubleValue();
        double additionalSimilarity = queryWeight * targetWeight;
        accumulator.addTo(targetID, additionalSimilarity);
      }
    }

    // add forgetting factor e^(-lambda*delta_T) and filter candidates < theta
    for (Iterator<Long2DoubleMap.Entry> it = accumulator.long2DoubleEntrySet().iterator(); it.hasNext();) {
      Long2DoubleMap.Entry e = it.next();
      final long deltaT = v.timestamp() - e.getLongKey();
      e.setValue(e.getDoubleValue() * forgetFactor(lambda, deltaT));
      if (Doubles.compare(e.getDoubleValue(), theta) < 0)
        it.remove();
    }

    return accumulator;
  }

  @Override
  public Vector addVector(final Vector v) {
    for (Entry e : v.int2DoubleEntrySet()) {
      int dimension = e.getIntKey();
      double weight = e.getDoubleValue();
      if (!idx.containsKey(dimension))
        idx.put(dimension, new PostingList());
      idx.get(dimension).add(v.timestamp(), weight);
      size++;
    }
    return Vector.EMPTY_VECTOR;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public String toString() {
    return "InvertedIndex [idx=" + idx + "]";
  }

  public static class PostingList implements Iterable<PostingEntry> {
    private LongArrayList ids = new LongArrayList();
    private DoubleArrayList weights = new DoubleArrayList();

    public void add(long vectorID, double weight) {
      ids.add(vectorID);
      weights.add(weight);
    }

    @Override
    public String toString() {
      return "[ids=" + ids + ", weights=" + weights + "]";
    }

    @Override
    public Iterator<PostingEntry> iterator() {
      return new Iterator<PostingEntry>() {
        private int i = 0;
        private PostingEntry entry = new PostingEntry();

        @Override
        public boolean hasNext() {
          return i < ids.size();
        }

        @Override
        public PostingEntry next() {
          entry.setKey(ids.getLong(i));
          entry.setValue(weights.getDouble(i));
          i++;
          return entry;
        }

        @Override
        public void remove() {
          i--;
          ids.removeLong(i);
          weights.removeDouble(i);
        }
      };
    }
  }

  static class PostingEntry {
    protected long key;
    protected double value;

    public PostingEntry() {
      this(0, 0);
    }

    public PostingEntry(long key, double value) {
      this.key = key;
      this.value = value;
    }

    public void setKey(long key) {
      this.key = key;
    }

    public void setValue(double value) {
      this.value = value;
    }

    public long getLongKey() {
      return key;
    }

    public double getDoubleValue() {
      return value;
    }

    @Override
    public String toString() {
      return "[" + key + " -> " + value + "]";
    }
  }
}
