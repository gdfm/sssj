package sssj.index;

import static sssj.base.Commons.forgettingFactor;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.Iterator;
import java.util.Map;

import sssj.base.Vector;

import com.google.common.primitives.Doubles;

public class InvertedIndex extends AbstractIndex {
  private Int2ReferenceMap<PostingList> idx = new Int2ReferenceOpenHashMap<>();
  private final double theta;
  private final double lambda;

  public InvertedIndex(double theta, double lambda) {
    this.theta = theta;
    this.lambda = lambda;
  }

  @Override
  public Map<Long, Double> queryWith(final Vector v, boolean addToIndex) {
    Long2DoubleOpenHashMap accumulator = new Long2DoubleOpenHashMap(size);
    for (Entry e : v.int2DoubleEntrySet()) {
      final int dimension = e.getIntKey();
      final double queryWeight = e.getDoubleValue();
      PostingList list;
      if ((list = idx.get(dimension)) != null) {
        for (PostingEntry pe : list) {
          numPostingEntries++;
          final long targetID = pe.getID();
          final double targetWeight = pe.getWeight();
          final double additionalSimilarity = queryWeight * targetWeight;
          accumulator.addTo(targetID, additionalSimilarity);
        }
      }
      if (addToIndex) {
        if (list == null) {
          list = new PostingList();
          idx.put(dimension, list);
        }
        list.add(v.timestamp(), queryWeight);
        size++;
      }
    }

    // add forgetting factor e^(-lambda*delta_T) and filter candidates < theta
    for (Iterator<Long2DoubleMap.Entry> it = accumulator.long2DoubleEntrySet().iterator(); it.hasNext();) {
      Long2DoubleMap.Entry e = it.next();
      final long deltaT = v.timestamp() - e.getLongKey();
      e.setValue(e.getDoubleValue() * forgettingFactor(lambda, deltaT));
      if (Doubles.compare(e.getDoubleValue(), theta) < 0)
        it.remove();
    }
    numCandidates += accumulator.size();
    numSimilarities = numCandidates;
    return accumulator;
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

    public int size() {
      return ids.size();
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
          entry.setID(ids.getLong(i));
          entry.setWeight(weights.getDouble(i));
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
    protected long id;
    protected double weight;

    public PostingEntry() {
      this(0, 0);
    }

    public PostingEntry(long key, double value) {
      this.id = key;
      this.weight = value;
    }

    public void setID(long id) {
      this.id = id;
    }

    public void setWeight(double weight) {
      this.weight = weight;
    }

    public long getID() {
      return id;
    }

    public double getWeight() {
      return weight;
    }

    @Override
    public String toString() {
      return "[" + id + " -> " + weight + "]";
    }
  }
}
