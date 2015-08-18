package sssj.index;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.Iterator;
import java.util.Map;

import sssj.Vector;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

public class InvertedIndex implements Index {
  private Int2ReferenceMap<PostingList> idx = new Int2ReferenceOpenHashMap<>();
  private final double theta;
  private int size = 0;

  public InvertedIndex(double theta) {
    this.theta = theta;
  }

  @Override
  public Map<Long, Double> queryWith(Vector v) {
    Long2DoubleMap accumulator = new Long2DoubleOpenHashMap(size);
    for (Entry e : v.int2DoubleEntrySet()) {
      int dimension = e.getIntKey();
      if (!idx.containsKey(dimension))
        idx.put(dimension, new PostingList()); //TODO no need for this? If the posting list is empty we should not create it here
      PostingList list = idx.get(dimension);
      double queryWeight = e.getDoubleValue();
      for (PostingEntry pe : list) {
        long targetID = pe.getLongKey();
        double targetWeight = pe.getDoubleValue();
        double currentSimilarity = accumulator.get(targetID);
        double additionalSimilarity = queryWeight * targetWeight;
        accumulator.put(targetID, currentSimilarity + additionalSimilarity);
      }
    }
    //    return accumulator;

    Map<Long, Double> results = Maps.filterValues(accumulator, new Predicate<Double>() {
      @Override
      public boolean apply(Double input) {
        return input.compareTo(theta) >= 0;
      }
    });
    return results;
  }

  @Override
  public Vector addVector(Vector v) {
    size++;
    for (Entry e : v.int2DoubleEntrySet()) {
      int dimension = e.getIntKey();
      double weight = e.getDoubleValue();
      if (!idx.containsKey(dimension))
        idx.put(dimension, new PostingList());
      idx.get(dimension).add(v.timestamp(), weight);
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
          ids.removeLong(i);
          weights.removeDouble(i);
        }
      };
    }
  }

  public static class PostingEntry {
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
