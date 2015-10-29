package sssj.index;

import static sssj.base.Commons.*;
import it.unimi.dsi.fastutil.BidirectionalIterator;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;

import org.apache.commons.math3.util.FastMath;

import sssj.base.CircularBuffer;
import sssj.base.StreamingResiduals;
import sssj.base.Vector;
import sssj.index.L2APIndex.L2APPostingEntry;
import sssj.index.StreamingPureL2APIndex.StreamingL2APPostingList.L2APPostingListIterator;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;

public class StreamingPureL2APIndex extends AbstractIndex {
  private final Int2ReferenceMap<StreamingL2APPostingList> idx = new Int2ReferenceOpenHashMap<>();
  private final StreamingResiduals residuals = new StreamingResiduals();
  private final Long2DoubleOpenHashMap ps = new Long2DoubleOpenHashMap();
  private final Long2DoubleOpenHashMap accumulator = new Long2DoubleOpenHashMap();
  private final Long2DoubleOpenHashMap matches = new Long2DoubleOpenHashMap();
  private final double theta;
  private final double lambda;
  private final double tau;

  public StreamingPureL2APIndex(double theta, double lambda) {
    this.theta = theta;
    this.lambda = lambda;
    this.tau = tau(theta, lambda);
    System.out.println("Tau = " + tau);
    precomputeFFTable(lambda, (int) Math.ceil(tau));
  }

  @Override
  public Map<Long, Double> queryWith(final Vector v, final boolean addToIndex) {
    accumulator.clear();
    matches.clear();
    /* candidate generation */
    generateCandidates(v);
    /* candidate verification */
    verifyCandidates(v);
    /* index building */
    if (addToIndex) {
      Vector residual = addToIndex(v);
      residuals.add(residual);
    }
    return matches;
  }

  private final void generateCandidates(final Vector v) {
    double l2remscore = 1, // rs4
    rst = 1, squaredQueryPrefixMagnitude = 1;

    for (BidirectionalIterator<Entry> vecIter = v.int2DoubleEntrySet().fastIterator(v.int2DoubleEntrySet().last()); vecIter
        .hasPrevious();) { // iterate over v in reverse order
      final Entry e = vecIter.previous();
      final int dimension = e.getIntKey();
      final double queryWeight = e.getDoubleValue(); // x_j
      // forgetting factor applied directly to the l2prefix bound
      final double rscore = l2remscore;
      squaredQueryPrefixMagnitude -= queryWeight * queryWeight;

      StreamingL2APPostingList list;
      if ((list = idx.get(dimension)) != null) {
        // TODO possibly size filtering: remove entries from the posting list with |y| < minsize (need to save size in the posting list)
        for (L2APPostingListIterator listIter = list.reverseIterator(); listIter.hasPrevious();) {
          numPostingEntries++;
          final L2APPostingEntry pe = listIter.previous();
          final long targetID = pe.getID(); // y

          // time filtering
          final long deltaT = v.timestamp() - targetID;
          if (Doubles.compare(deltaT, tau) > 0) {
            listIter.next(); // back off one position
            listIter.cutHead();
            size -= listIter.nextIndex();
            continue;
          }

          final double ff = forgettingFactor(lambda, deltaT);
          if (accumulator.containsKey(targetID) || Double.compare(rscore, theta) >= 0) {
            final double targetWeight = pe.getWeight(); // y_j
            final double additionalSimilarity = queryWeight * targetWeight; // x_j * y_j
            accumulator.addTo(targetID, additionalSimilarity); // A[y] += x_j * y_j
            final double l2bound = accumulator.get(targetID) + FastMath.sqrt(squaredQueryPrefixMagnitude)
                * pe.magnitude; // A[y] + ||x'_j|| * ||y'_j||
            // forgetting factor applied directly to the l2sum bound
            if (Double.compare(l2bound * ff, theta) < 0) {
              accumulator.remove(targetID); // prune this candidate (early verification)
            }
          }
        }
        rst -= queryWeight * queryWeight; // rs_t -= x_j^2
        l2remscore = FastMath.sqrt(rst); // rs_4 = sqrt(rs_t)
      }
    }
    numCandidates += accumulator.size();
  }

  private final void verifyCandidates(final Vector v) {
    for (Long2DoubleMap.Entry e : accumulator.long2DoubleEntrySet()) {
      // TODO possibly use size filtering (sz_3)
      final long candidateID = e.getLongKey();
      final long deltaT = v.timestamp() - candidateID;
      if (deltaT > tau) // time pruning
        continue;
      final double ff = forgettingFactor(lambda, deltaT);
      if (Double.compare((e.getDoubleValue() + ps.get(candidateID)) * ff, theta) < 0) // A[y] = dot(x, y")
        continue; // l2 pruning

      final long lowWatermark = (long) Math.floor(v.timestamp() - tau);
      final Vector residual = residuals.getAndPrune(candidateID, lowWatermark); // TODO prune also ps?
      assert (residual != null) : "Residual not found. TS=" + v.timestamp() + " candidateID=" + candidateID;
      final double dpscore = e.getDoubleValue()
          + Math.min(v.maxValue() * residual.size(), residual.maxValue() * v.size());
      if (Double.compare(dpscore * ff, theta) < 0)
        continue; // dpscore, eq. (5)

      double score = e.getDoubleValue() + Vector.similarity(v, residual); // dot(x, y) = A[y] + dot(x, y')
      score *= ff; // apply forgetting factor
      numSimilarities++;
      if (Double.compare(score, theta) >= 0) // final check
        matches.put(candidateID, score);
    }
  }

  private final Vector addToIndex(final Vector v) {
    double bt = 0, b3 = 0, pscore = 0;
    boolean psSaved = false;
    final Vector residual = new Vector(v.timestamp());

    for (Entry e : v.int2DoubleEntrySet()) {
      final int dimension = e.getIntKey();
      final double weight = e.getDoubleValue();

      pscore = b3;
      bt += weight * weight;
      b3 = FastMath.sqrt(bt);

      // forgetting factor applied directly bounds
      if (Double.compare(b3, theta) >= 0) { // bound larger than threshold, start indexing
        if (!psSaved) {
          ps.put(v.timestamp(), pscore);
          psSaved = true;
        }
        StreamingL2APPostingList list;
        if ((list = idx.get(dimension)) == null) {
          list = new StreamingL2APPostingList();
          idx.put(dimension, list);
        }
        list.add(v.timestamp(), weight, b3);
        size++;
      } else {
        residual.put(dimension, weight);
      }
    }
    return residual;
  }

  @Override
  public String toString() {
    return "StreamingPureL2APIndex [idx=" + idx + ", residuals=" + residuals + ", ps=" + ps + "]";
  }

  public static class StreamingL2APPostingList implements Iterable<L2APPostingEntry> {
    private final CircularBuffer ids = new CircularBuffer(); // longs
    private final CircularBuffer weights = new CircularBuffer(); // doubles
    private final CircularBuffer magnitudes = new CircularBuffer(); // doubles

    public void add(long vectorID, double weight, double magnitude) {
      ids.pushLong(vectorID);
      weights.pushDouble(weight);
      magnitudes.pushDouble(magnitude);
    }

    public int size() {
      return ids.size();
    }

    @Override
    public String toString() {
      return "[ids=" + ids + ", weights=" + weights + ", magnitudes=" + magnitudes + "]";
    }

    @Override
    public Iterator<L2APPostingEntry> iterator() {
      return new L2APPostingListIterator();
    }

    public L2APPostingListIterator reverseIterator() {
      return new L2APPostingListIterator(size());
    }

    class L2APPostingListIterator implements ListIterator<L2APPostingEntry> {
      private final L2APPostingEntry entry = new L2APPostingEntry();
      private int i;

      public L2APPostingListIterator() {
        this(0);
      }

      public L2APPostingListIterator(int start) {
        Preconditions.checkArgument(i >= 0);
        Preconditions.checkArgument(i <= size());
        this.i = start;
      }

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
      public int nextIndex() {
        return i;
      }

      @Override
      public boolean hasPrevious() {
        return i > 0;
      }

      @Override
      public L2APPostingEntry previous() {
        i--;
        entry.setID(ids.peekLong(i));
        entry.setWeight(weights.peekDouble(i));
        entry.setMagnitude(magnitudes.peekDouble(i));
        return entry;
      }

      @Override
      public int previousIndex() {
        return i - 1;
      }

      @Override
      public void remove() {
        i--;
        assert (i == 0); // removal always happens at the head
        ids.popLong();
        weights.popDouble();
        magnitudes.popDouble();
      }

      public void cutHead() {
        ids.trimHead(i);
        weights.trimHead(i);
        magnitudes.trimHead(i);
        i = 0;
      }

      @Override
      public void set(L2APPostingEntry e) {
        throw new UnsupportedOperationException("Entries in the list are immutable");
      }

      @Override
      public void add(L2APPostingEntry e) {
        throw new UnsupportedOperationException("Entries can only be added at the end of the list");
      }
    };
  }
}