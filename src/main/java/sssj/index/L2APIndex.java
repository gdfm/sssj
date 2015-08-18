package sssj.index;

import it.unimi.dsi.fastutil.BidirectionalIterator;
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

public class L2APIndex implements Index {
  private Int2ReferenceMap<L2APPostingList> idx = new Int2ReferenceOpenHashMap<>();
  private ResidualList resList = new ResidualList();
  private Long2DoubleMap ps = new Long2DoubleOpenHashMap();
  private int size = 0;
  private final double theta;
  private final Vector maxVectorInWindow; // c_w
  private final Vector maxVectorInIndex; // \hat(c_w)

  public L2APIndex(double theta, Vector maxVector) {
    this.theta = theta;
    this.maxVectorInWindow = maxVector;
    this.maxVectorInIndex = new Vector();
  }

  @Override
  public Map<Long, Double> queryWith(Vector v) {
    Long2DoubleOpenHashMap matches = new Long2DoubleOpenHashMap();
    Long2DoubleOpenHashMap accumulator = new Long2DoubleOpenHashMap(size);
    // int minSize = theta / rw_x; //TODO possibly size filtering (need to sort dataset by max row weight rw_x)
    double remscore = Vector.similarity(v, maxVectorInIndex); // rs3, enhanced remscore bound
    double l2remscore = 1, // rs4
    rst = 1, squaredQueryPrefixMagnitude = 1;

    /* candidate generation */
    for (BidirectionalIterator<Entry> it = v.int2DoubleEntrySet().fastIterator(
        v.int2DoubleEntrySet().last()); it.hasPrevious();) { // iterate over v in reverse order
      Entry e = it.previous();
      int dimension = e.getIntKey();
      double queryWeight = e.getDoubleValue(); // x_j

      if (!idx.containsKey(dimension))
        idx.put(dimension, new L2APPostingList());
      L2APPostingList list = idx.get(dimension);
      //TODO possibly size filtering: remove entries from the posting list with |y| < minsize (need to save size in the posting list)

      squaredQueryPrefixMagnitude -= queryWeight * queryWeight;
      for (L2APPostingEntry pe : list) {
        long targetID = pe.getID(); // y
        double rscore = Math.min(remscore, l2remscore);
        if (accumulator.containsKey(targetID) || Double.compare(rscore, theta) >= 0) {
          double targetWeight = pe.getWeight(); // y_j
          double additionalSimilarity = queryWeight * targetWeight; // x_j * y_j 
          // TODO add e^(-lambda*delta_t)
          accumulator.addTo(targetID, additionalSimilarity); // A[y] += x_j * y_j 
          if (Double.compare(accumulator.get(targetID) + Math.sqrt(squaredQueryPrefixMagnitude) * pe.magnitude, theta) < 0) // A[y] + ||x'_j|| * ||y'_j||
            accumulator.remove(targetID); // prune this candidate (early verification)
        }
      }
      remscore -= queryWeight * maxVectorInIndex.get(dimension); // rs_3 -= x_j * \hat{c_w}
      rst -= queryWeight * queryWeight; // rs_t -= x_j^2
      l2remscore = Math.sqrt(rst); // rs_4 = sqrt(rs_t)
    }

    /* candidate verification */
    for (Long2DoubleMap.Entry e : accumulator.long2DoubleEntrySet()) {
      //TODO possibly use size filtering (sz_3)
      long candidateID = e.getLongKey();
      if (Double.compare(e.getDoubleValue() + ps.get(candidateID), theta) < 0) // A[y] = dot(x, y'')
        continue; // l2 pruning
      Vector residual = resList.get(candidateID);
      assert (residual != null);
      double dpscore = e.getDoubleValue() + Math.min(v.maxValue() * residual.size(), residual.maxValue() * v.size());
      if (Double.compare(dpscore, theta) < 0)
        continue; // dpscore, eq. (5)

      double score = e.getDoubleValue() + Vector.similarity(v, residual); // dot(x, y) = A[y] + dot(x, y')

      if (Double.compare(score, theta) >= 0) // final check
        matches.put(candidateID, score);
    }
    return matches;
  }

  @Override
  public Vector addVector(Vector v) {
    size++;
    double b1 = 0, bt = 0, b3 = 0, pscore = 0;
    boolean psSaved = false;
    Vector residual = new Vector(v.timestamp());
    for (Entry e : v.int2DoubleEntrySet()) {
      int dimension = e.getIntKey();
      double weight = e.getDoubleValue();

      pscore = Math.min(b1, b3);
      b1 += weight * maxVectorInWindow.get(dimension);
      bt += weight * weight;
      b3 = Math.sqrt(bt);

      if (Double.compare(Math.min(b1, b3), theta) >= 0) {
        if (!psSaved) {
          assert (!ps.containsKey(v.timestamp()));
          ps.put(v.timestamp(), pscore);
          psSaved = true;
        }
        if (!idx.containsKey(dimension))
          idx.put(dimension, new L2APPostingList());
        idx.get(dimension).add(v.timestamp(), weight, b3); //TODO check correctness
      } else {
        residual.put(dimension, weight);
      }
    }
    resList.add(residual);
    Vector.updateMaxByDimension(maxVectorInIndex, v); //TODO check that this is the right place to update the max, L2AP performs the update at the end of queryWith()
    return residual;

  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public String toString() {
    return "L2APIndex [idx=" + idx + ", resList=" + resList + ", ps=" + ps + "]";
  }

  public static class L2APPostingList implements Iterable<L2APPostingEntry> {
    private LongArrayList ids = new LongArrayList();
    private DoubleArrayList weights = new DoubleArrayList();
    private DoubleArrayList magnitudes = new DoubleArrayList();

    public void add(long vectorID, double weight, double magnitude) {
      ids.add(vectorID);
      weights.add(weight);
      magnitudes.add(magnitude);
    }

    @Override
    public String toString() {
      return "[ids=" + ids + ", weights=" + weights + ", magnitudes=" + magnitudes + "]";
    }

    @Override
    public Iterator<L2APPostingEntry> iterator() {
      return new Iterator<L2APPostingEntry>() {
        private int i = 0;
        private L2APPostingEntry entry = new L2APPostingEntry();

        @Override
        public boolean hasNext() {
          return i < ids.size();
        }

        @Override
        public L2APPostingEntry next() {
          entry.setID(ids.getLong(i));
          entry.setWeight(weights.getDouble(i));
          entry.setMagnitude(magnitudes.getDouble(i));
          i++;
          return entry;
        }

        @Override
        public void remove() {
          ids.removeLong(i);
          weights.removeDouble(i);
          magnitudes.removeDouble(i);
        }
      };
    }
  }

  public static class L2APPostingEntry {
    protected long id;
    protected double weight;
    protected double magnitude;

    public L2APPostingEntry() {
      this(0, 0, 0);
    }

    public L2APPostingEntry(long id, double weight, double magnitude) {
      this.id = id;
      this.weight = weight;
      this.magnitude = magnitude;
    }

    public void setID(long id) {
      this.id = id;
    }

    public void setWeight(double weight) {
      this.weight = weight;
    }

    public void setMagnitude(double magnitude) {
      this.magnitude = magnitude;
    }

    public long getID() {
      return id;
    }

    public double getWeight() {
      return weight;
    }

    public double getMagnitude() {
      return magnitude;
    }

    @Override
    public String toString() {
      return "[" + id + " -> " + weight + " (" + magnitude + ")]";
    }
  }

}