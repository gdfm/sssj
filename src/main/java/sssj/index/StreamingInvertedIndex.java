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
import sssj.base.Vector;
import sssj.index.InvertedIndex.PostingEntry;

import com.google.common.primitives.Doubles;

public class StreamingInvertedIndex implements Index {
  private Int2ReferenceMap<StreamingPostingList> idx = new Int2ReferenceOpenHashMap<>();
  private final Long2DoubleOpenHashMap accumulator = new Long2DoubleOpenHashMap();
  private int size = 0;
  private final double theta;
  private final double lambda;
  private final double tau;

  public StreamingInvertedIndex(double theta, double lambda) {
    this.theta = theta;
    this.lambda = lambda;
    this.tau = tau(theta, lambda);
    // this.maxVector = new Vector();
    System.out.println("Tau = " + tau);
    precomputeFFTable(lambda, (int) Math.ceil(tau));
  }

  @Override
  public Map<Long, Double> queryWith(final Vector v, boolean addToIndex) {
    accumulator.clear();
    for (Int2DoubleMap.Entry e : v.int2DoubleEntrySet()) {
      final int dimension = e.getIntKey();
      final double queryWeight = e.getDoubleValue();
      StreamingPostingList list;
      if ((list = idx.get(dimension)) != null) {
        for (Iterator<PostingEntry> it = list.iterator(); it.hasNext();) {
          final PostingEntry pe = it.next();
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
        if (addToIndex) {
          list = new StreamingPostingList();
          idx.put(dimension, list);
        }
      }
      if (addToIndex) {
        list.add(v.timestamp(), queryWeight);
        size++;
      }
    }

    // filter candidates < theta
    for (Iterator<Long2DoubleMap.Entry> it = accumulator.long2DoubleEntrySet().iterator(); it.hasNext();)
      if (Doubles.compare(it.next().getDoubleValue(), theta) < 0)
        it.remove();

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

  static class StreamingPostingList implements Iterable<PostingEntry> {
    private CircularBuffer ids = new CircularBuffer(); // longs
    private CircularBuffer weights = new CircularBuffer(); // doubles

    public void add(long vectorID, double weight) {
      ids.pushLong(vectorID);
      weights.pushDouble(weight);
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
      return new StreamingPostingListIterator();
    }

    class StreamingPostingListIterator implements Iterator<PostingEntry> {
      private final PostingEntry entry = new PostingEntry();
      private int i = 0;

      @Override
      public boolean hasNext() {
        return i < ids.size();
      }

      @Override
      public PostingEntry next() {
        entry.setID(ids.peekLong(i));
        entry.setWeight(weights.peekDouble(i));
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
}
