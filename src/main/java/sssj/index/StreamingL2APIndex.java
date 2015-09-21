package sssj.index;

import static sssj.base.Commons.*;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.util.Iterator;
import java.util.Map;

import sssj.base.CircularBuffer;
import sssj.base.Commons.ResidualList;
import sssj.base.Vector;
import sssj.index.L2APIndex.L2APPostingEntry;

import com.google.common.primitives.Doubles;

public class StreamingL2APIndex implements Index {
// private static final Logger log = LoggerFactory.getLogger(StreamingL2APIndex.class);
  private Int2ReferenceMap<StreamingL2APPostingList> idx = new Int2ReferenceOpenHashMap<>();
  private ResidualList resList = new ResidualList();
  private final Long2DoubleOpenHashMap accumulator = new Long2DoubleOpenHashMap();
  private int size = 0;
  private final double theta;
  private final double lambda;
  private final double tau;
  private final Vector maxVector; // \hat{y}

  public StreamingL2APIndex(double theta, double lambda) {
    this.theta = theta;
    this.lambda = lambda;
    this.tau = tau(theta, lambda);
    this.maxVector = new Vector();
    // this.maxVector = new Vector();
    System.out.println("Tau = " + tau);
    precomputeFFTable(lambda, (int) Math.ceil(tau));
  }

  @Override
  public Map<Long, Double> queryWith(final Vector v, boolean addToIndex) {
    // Vector updates = maxVector.updateMaxByDimension(v);
    accumulator.clear();
    for (Int2DoubleMap.Entry e : v.int2DoubleEntrySet()) {
      final int dimension = e.getIntKey();
      final double queryWeight = e.getDoubleValue();
      StreamingL2APPostingList list;
      if ((list = idx.get(dimension)) != null) {
        for (Iterator<L2APPostingEntry> it = list.iterator(); it.hasNext();) {
          final L2APPostingEntry pe = it.next();
          final long targetID = pe.getID();

          // time filtering
          final long deltaT = v.timestamp() - targetID;
          if (Doubles.compare(deltaT, tau) > 0) {
            it.remove();
            size--;
            continue;
          }

          final double targetWeight = pe.getWeight();
          final double additionalSimilarity = queryWeight * targetWeight * forgettingFactor(lambda, deltaT);
          accumulator.addTo(targetID, additionalSimilarity);
        }
      } else {
        list = new StreamingL2APPostingList();
        idx.put(dimension, list);
      }
      list.add(v.timestamp(), queryWeight);
      size++;
    }

    // filter candidates < theta
    for (Iterator<Long2DoubleMap.Entry> it = accumulator.long2DoubleEntrySet().iterator(); it.hasNext();) {
      if (Doubles.compare(it.next().getDoubleValue(), theta) < 0)
        it.remove();
    }

    return accumulator;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public String toString() {
    return "StreamingInvertedIndex [idx=" + idx + "]";
  }

  static class StreamingL2APPostingList implements Iterable<L2APPostingEntry> {
    private CircularBuffer ids = new CircularBuffer(); // longs
    private CircularBuffer weights = new CircularBuffer(); // doubles
    private CircularBuffer magnitudes = new CircularBuffer(); // doubles

    public void add(long vectorID, double weight) {
      ids.pushLong(vectorID);
      weights.pushDouble(weight);
    }

    @Override
    public String toString() {
      return "[ids=" + ids + ", weights=" + weights + "]";
    }

    @Override
    public Iterator<L2APPostingEntry> iterator() {
      return new StreamingPostingListIterator();
    }

    class StreamingPostingListIterator implements Iterator<L2APPostingEntry> {
      private final L2APPostingEntry entry = new L2APPostingEntry();
      private int i = 0;

      @Override
      public boolean hasNext() {
        return i < ids.size();
      }

      @Override
      public L2APPostingEntry next() {
        entry.setID(ids.peekLong(i));
        entry.setWeight(weights.peekDouble(i));
        entry.setMagnitude(magnitudes.peekDouble(i));
        i++;
        return entry;
      }

      @Override
      public void remove() {
        i--;
        assert (i == 0); // removals always happen at the head
        ids.popLong();
        weights.popDouble();
        magnitudes.popDouble();
      }
    }
  }
}
