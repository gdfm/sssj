package sssj.index;

import static sssj.base.Commons.*;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sssj.base.CircularBuffer;
import sssj.base.Commons;
import sssj.base.Vector;

import com.google.common.primitives.Doubles;

public class StreamingIndex implements Index {
  private static final Logger log = LoggerFactory.getLogger(StreamingIndex.class);
  private Int2ReferenceMap<StreamingPostingList> idx = new Int2ReferenceOpenHashMap<>();
  // private ResidualList resList = new ResidualList();
  private final Long2DoubleOpenHashMap accumulator = new Long2DoubleOpenHashMap();
  private int size = 0;
  private final double theta;
  private final double lambda;
  private final double tau;

  // private final Vector maxVector; // \hat{y}

  public StreamingIndex(double theta, double lambda) {
    this.theta = theta;
    this.lambda = lambda;
    this.tau = tau(theta, lambda);
    // this.maxVector = new Vector();
    System.out.println("Tau = " + tau);
    precomputeFFTable(lambda, (int) Math.ceil(tau));
  }

  @Override
  public Map<Long, Double> queryWith(final Vector v) { // FIXME can we scan the vector only once and already add it here?
    // Vector updates = maxVector.updateMaxByDimension(v);
    accumulator.clear();
    for (Int2DoubleMap.Entry e : v.int2DoubleEntrySet()) {
      final int dimension = e.getIntKey();
      final double queryWeight = e.getDoubleValue();
      if (idx.containsKey(dimension)) {
        final StreamingPostingList list = idx.get(dimension);
        for (Iterator<StreamingPostingEntry> it = list.iterator(); it.hasNext();) {
          final StreamingPostingEntry pe = it.next();
          final long targetID = pe.getLongKey();

          // time filtering
          final long deltaT = v.timestamp() - targetID;
          if (Doubles.compare(deltaT, tau) > 0) {
            it.remove();
            size--;
            continue;
          }

          final double targetWeight = pe.getDoubleValue();
          final double additionalSimilarity = queryWeight * targetWeight * forgetFactor(lambda, deltaT);
          accumulator.addTo(targetID, additionalSimilarity);
        }
      } else {
        idx.put(dimension, new StreamingPostingList());
      }
      idx.get(dimension).add(v.timestamp(), queryWeight);
    }

    // filter candidates < theta
    for (Iterator<Long2DoubleMap.Entry> it = accumulator.long2DoubleEntrySet().iterator(); it.hasNext();) {
      if (Doubles.compare(it.next().getDoubleValue(), theta) < 0)
        it.remove();
    }

    return accumulator;
  }

  @Override
  public Vector addVector(final Vector v) {
    // do nothing
    return Vector.EMPTY_VECTOR;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public String toString() {
    return "StreamingIndex [idx=" + idx + "]";
  }

  static class StreamingPostingList implements Iterable<StreamingPostingEntry> {
    private CircularBuffer ids = new CircularBuffer();
    private CircularBuffer weights = new CircularBuffer();

    public void add(long vectorID, double weight) {
      ids.pushLong(vectorID);
      weights.pushDouble(weight);
    }

    @Override
    public String toString() {
      return "[ids=" + ids + ", weights=" + weights + "]";
    }

    @Override
    public Iterator<StreamingPostingEntry> iterator() {
      return new StreamingPostingListIterator();
    }

    class StreamingPostingListIterator implements Iterator<StreamingPostingEntry> {
      private final StreamingPostingEntry entry = new StreamingPostingEntry();
      private int i = 0;

      @Override
      public boolean hasNext() {
        return i < ids.size();
      }

      @Override
      public StreamingPostingEntry next() {
        entry.setKey(ids.peekLong(i));
        entry.setValue(weights.peekDouble(i));
        i++;
        return entry;
      }

      @Override
      public void remove() {
        i--;
        assert (i == 0); // removals always happen at the head
        ids.popLong();
        weights.popDouble();
      }
    }
  }

  static class StreamingPostingEntry {
    private long key;
    private double value;

    public StreamingPostingEntry() {
      this(0, 0);
    }

    public StreamingPostingEntry(long key, double value) {
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
