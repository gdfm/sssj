package sssj;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.Iterator;
import java.util.Map;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

public class AllPairsIndex implements Index {
  private Int2ReferenceMap<PostingList> idx = new Int2ReferenceOpenHashMap<>();
  private final double threshold;
  private final Vector maxVector = new Vector();
  private int size = 0;

  public AllPairsIndex(double threshold) {
    this.threshold = threshold;
  }

  @Override
  public Map<Long, Double> queryWith(Vector v) {
    Long2DoubleMap accumulator = new Long2DoubleOpenHashMap(size);
    for (Entry e : v.int2DoubleEntrySet()) {
      int dimension = e.getIntKey();
      if (!idx.containsKey(dimension))
        idx.put(dimension, new PostingList());
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

    Map<Long, Double> results = Maps.filterValues(accumulator, new Predicate<Double>() { // TODO should not be needed
          @Override
          public boolean apply(Double input) {
            return input.compareTo(threshold) >= 0;
          }
        });
    return results;
  }

  @Override
  public Vector addVector(Vector v) {
    size++;
    Vector toReindex = Vector.maxByDimension(maxVector, v);
    //TODO re-index vectors in posting lists corresponding to dimensions in toReindex
    for (Entry e : v.int2DoubleEntrySet()) {
      int dimension = e.getIntKey();
      double weight = e.getDoubleValue();
      if (!idx.containsKey(dimension))
        idx.put(dimension, new PostingList());
      idx.get(dimension).add(v.timestamp(), weight);
    }
    return Vector.EMPTY_VECTOR;
  }

  public int getSize() {
    return size();
  }

  public int size() {
    return size;
  }

  public static class PostingList implements Iterable<PostingEntry> {
    private LongArrayList vids = new LongArrayList();
    private DoubleArrayList weights = new DoubleArrayList();

    public void add(long vectorID, double weight) {
      vids.add(vectorID);
      weights.add(weight);
    }

    @Override
    public Iterator<PostingEntry> iterator() {
      return new Iterator<PostingEntry>() {
        private int i = 0;
        private PostingEntry entry = new PostingEntry();

        @Override
        public boolean hasNext() {
          return i < vids.size();
        }

        @Override
        public PostingEntry next() {
          entry.setKey(vids.getLong(i));
          entry.setValue(weights.getDouble(i));
          i++;
          return entry;
        }

        @Override
        public void remove() {
          vids.removeLong(i);
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
  }
}
