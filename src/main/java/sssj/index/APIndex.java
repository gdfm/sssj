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

public class APIndex implements Index {
  private Int2ReferenceMap<PostingList> idx = new Int2ReferenceOpenHashMap<>();
  private ResidualList resList = new ResidualList();
  private final double theta;
  private final Vector maxVector;
  private int size = 0;

  public APIndex(double threshold, Vector maxVector) {
    this.theta = threshold;
    this.maxVector = maxVector;
  }

  @Override
  public Map<Long, Double> queryWith(Vector v) {
    Long2DoubleOpenHashMap matches = new Long2DoubleOpenHashMap();
    Long2DoubleOpenHashMap accumulator = new Long2DoubleOpenHashMap(size);
    // int minSize = theta / rw_x; //TODO possibly size filtering (need to sort dataset by max row value rw_x)
    double remscore = Vector.similarity(maxVector, v);
    for (Entry e : v.int2DoubleEntrySet()) {
      int dimension = e.getIntKey();
      if (!idx.containsKey(dimension))
        idx.put(dimension, new PostingList());
      PostingList list = idx.get(dimension);
      //TODO possibly size filtering: remove entries from the posting list with |y| < minsize (need to save size in the posting list)
      double queryWeight = e.getDoubleValue(); // x_j
      for (PostingEntry pe : list) {
        long targetID = pe.getLongKey(); // y
        if (accumulator.containsKey(targetID) || Double.compare(remscore, theta) >= 0) {
          double targetWeight = pe.getDoubleValue(); // y_j
          //double currentSimilarity = accumulator.get(targetID);
          double additionalSimilarity = queryWeight * targetWeight; // x_j * y_j 
          // TODO add e^(-lambda*delta_t)
          accumulator.addTo(targetID, additionalSimilarity); // A[y] += x_j * y_j 
          //accumulator.put(targetID, currentSimilarity + additionalSimilarity);
        }
        remscore -= queryWeight * maxVector.get(dimension);
      }
    }
    //    return accumulator;

    for (Long2DoubleMap.Entry e : accumulator.long2DoubleEntrySet()) {
      //TODO possibly use size filtering (sz_3)
      long candidateID = e.getLongKey();
      Vector candidateResidual = resList.get(candidateID);
      assert (candidateResidual != null);
      double score = e.getDoubleValue() + Vector.similarity(candidateResidual, v); // A[y] + dot(y',x)
      if (Double.compare(score, theta) >= 0)
        matches.put(candidateID, score);
    }
    return matches;
  }

  @Override
  public Vector addVector(Vector v) {
    size++;
    double pscore = 0;
    Vector residual = new Vector(v.timestamp());
    for (Entry e : v.int2DoubleEntrySet()) {
      int dimension = e.getIntKey();
      double weight = e.getDoubleValue();
      pscore += weight * maxVector.get(dimension);
      if (Double.compare(pscore, theta) >= 0) {
        if (!idx.containsKey(dimension))
          idx.put(dimension, new PostingList());
        idx.get(dimension).add(v.timestamp(), weight);
        // v.remove(dimension);
      } else {
        residual.put(dimension, weight);
      }
    }
    resList.add(residual);
    return residual;
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
        private final PostingEntry entry = new PostingEntry();

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
