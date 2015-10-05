package sssj.index;

import static sssj.base.Commons.*;
import it.unimi.dsi.fastutil.BidirectionalIterator;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.util.FastMath;

import sssj.base.CircularBuffer;
import sssj.base.ResidualList;
import sssj.base.StreamingMaxVector;
import sssj.base.Vector;
import sssj.index.L2APIndex.L2APPostingEntry;

import com.google.common.primitives.Doubles;

public class StreamingL2APIndex implements Index {
  private final Int2ReferenceMap<StreamingL2APPostingList> idx = new Int2ReferenceOpenHashMap<>();
  private final ResidualList resList = new ResidualList();
  private final Long2DoubleOpenHashMap ps = new Long2DoubleOpenHashMap();
  private final Long2DoubleOpenHashMap accumulator = new Long2DoubleOpenHashMap();
  private final Long2DoubleOpenHashMap matches = new Long2DoubleOpenHashMap();
  private final double theta;
  private final double lambda;
  private final double tau;
  private final StreamingMaxVector maxVector; // \hat{c_w}
  private int size = 0;

  public StreamingL2APIndex(double theta, double lambda) {
    this.theta = theta;
    this.lambda = lambda;
    this.maxVector = new StreamingMaxVector(lambda);
    this.tau = tau(theta, lambda);
    System.out.println("Tau = " + tau);
    precomputeFFTable(lambda, (int) Math.ceil(tau));
  }

  @Override
  public Map<Long, Double> queryWith(final Vector v, final boolean addToIndex) {
    accumulator.clear();
    matches.clear();
    Vector updates = maxVector.updateMaxByDimension(v);
    /* reindexing */
    if (updates.size() > 0)
      reindex(updates);
    /* candidate generation */
    generateCandidates(v);
    /* candidate verification */
    verifyCandidates(v);
    /* index building */
    if (addToIndex) {
      Vector residual = addToIndex(v);
      resList.add(residual);
    }
    return matches;
  }

  private final void reindex(Vector updates) {
    List<Vector> newRes = new LinkedList<>();
    for (Iterator<Vector> it = resList.iterator(); it.hasNext();) {
      final Vector r = it.next();
      final double simDelta = Vector.similarity(updates, r);
      if (simDelta > 0) {
        final double pscore = ps.get(r.timestamp());
        if (pscore + simDelta > theta) {
          final Vector newResidual = this.addToIndex(r);
          it.remove();
          newRes.add(newResidual);
        }
      }
    }
    for (Vector r : newRes)
      resList.add(r);
  }

  private final void generateCandidates(final Vector v) {
    // lower bound on the forgetting factor w.r.t. the maximum vector
    final long minDeltaT = v.timestamp() - maxVector.timestamp();
// if (Doubles.compare(minDeltaT, tau) > 0) // time filtering // FIXME ff
// return;
// final double maxff = forgettingFactor(lambda, minDeltaT);
    // rs3, enhanced remscore bound with addded forgetting factor, for Streaming maxVector is the max vector in the index
    double remscore = Vector.similarity(v, maxVector); // rs3, enhanced remscore bound
    double l2remscore = 1, // rs4
    rst = 1, squaredQueryPrefixMagnitude = 1;

    boolean keepFiltering = true;
    for (BidirectionalIterator<Entry> vecIter = v.int2DoubleEntrySet().fastIterator(v.int2DoubleEntrySet().last()); vecIter
        .hasPrevious();) { // iterate over v in reverse order
      final Entry e = vecIter.previous();
      final int dimension = e.getIntKey();
      final double queryWeight = e.getDoubleValue(); // x_j
      // forgetting factor applied directly to the prefix and l2prefix bounds
// final double rscore = Math.min(remscore, l2remscore) * maxff; // FIXME ff
      final double rscore = Math.min(remscore, l2remscore);
      squaredQueryPrefixMagnitude -= queryWeight * queryWeight;

      StreamingL2APPostingList list;
      if ((list = idx.get(dimension)) != null) {
        // TODO possibly size filtering: remove entries from the posting list with |y| < minsize (need to save size in the posting list)
        for (Iterator<L2APPostingEntry> listIter = list.iterator(); listIter.hasNext();) {
          final L2APPostingEntry pe = listIter.next();
          final long targetID = pe.getID(); // y

          // time filtering
          boolean filtered = false;
          final long deltaT = v.timestamp() - targetID;
          if (Doubles.compare(deltaT, tau) > 0 && keepFiltering) {
            listIter.remove();
            size--;
            filtered = true;
            continue;
          }
          keepFiltering &= filtered; // keep filtering only if we have just filtered

          final double ff = forgettingFactor(lambda, deltaT);
          if (accumulator.containsKey(targetID) || Double.compare(rscore, theta) >= 0) {
            final double targetWeight = pe.getWeight(); // y_j
            final double additionalSimilarity = queryWeight * targetWeight; // x_j * y_j
            accumulator.addTo(targetID, additionalSimilarity); // A[y] += x_j * y_j
            final double l2bound = accumulator.get(targetID) + FastMath.sqrt(squaredQueryPrefixMagnitude)
                * pe.magnitude; // A[y] + ||x'_j|| * ||y'_j||
            // forgetting factor applied directly to the l2bound
            if (Double.compare(l2bound * ff, theta) < 0) // FIXME ff
              accumulator.remove(targetID); // prune this candidate (early verification)
          }
        }
        remscore -= queryWeight * maxVector.get(dimension); // rs_3 -= x_j * \hat{c_w}
        rst -= queryWeight * queryWeight; // rs_t -= x_j^2
        l2remscore = FastMath.sqrt(rst); // rs_4 = sqrt(rs_t)
      }
    }
  }

  private final void verifyCandidates(final Vector v) {
    for (Long2DoubleMap.Entry e : accumulator.long2DoubleEntrySet()) {
      // TODO possibly use size filtering (sz_3)
      final long candidateID = e.getLongKey();
      final long deltaT = v.timestamp() - candidateID;
      if (deltaT > tau) // time filtering // FIXME ff (should not even be needed)
        continue;
      final double ff = forgettingFactor(lambda, deltaT);

// if (Double.compare((e.getDoubleValue() + ps.get(candidateID)) * ff, theta) < 0) // A[y] = dot(x, y'') // FIXME ff
      if (Double.compare((e.getDoubleValue() + ps.get(candidateID)), theta) < 0) // A[y] = dot(x, y'')
        continue; // l2 pruning

      final long lowWatermark = (long) Math.floor(v.timestamp() - tau);
      final Vector residual = resList.getAndPrune(candidateID, lowWatermark); // TODO prune also ps?
      assert (residual != null);
      final double dpscore = e.getDoubleValue()
          + Math.min(v.maxValue() * residual.size(), residual.maxValue() * v.size());
// if (Double.compare(dpscore * ff, theta) < 0) // FIXME ff
      if (Double.compare(dpscore, theta) < 0)
        continue; // dpscore, eq. (5)

      double score = e.getDoubleValue() + Vector.similarity(v, residual); // dot(x, y) = A[y] + dot(x, y')
      score *= ff; // apply forgetting factor // FIXME ff
      if (Double.compare(score, theta) >= 0) // final check
        matches.put(candidateID, score);
    }
  }

  private final Vector addToIndex(final Vector v) {
    double b1 = 0, bt = 0, b3 = 0, pscore = 0;
    boolean psSaved = false;
    final Vector residual = new Vector(v.timestamp());
    // upper bound on the forgetting factor w.r.t. the maximum vector
    // TODO can be tightened with a deltaT per dimension
// final long maxDeltaT = v.timestamp() - maxVector.timestamp();
// final double maxff = forgettingFactor(lambda, maxDeltaT);
    // FIXME maxDeltaT can be larger than tau. How do we use it?

    for (Entry e : v.int2DoubleEntrySet()) {
      final int dimension = e.getIntKey();
      final double weight = e.getDoubleValue();

      pscore = Math.min(b1, b3);
      b1 += weight * maxVector.get(dimension);
      bt += weight * weight;
      b3 = FastMath.sqrt(bt);

      // forgetting factor applied directly bounds
// if (Double.compare(Math.min(b1, b3) * maxff, theta) >= 0) { // FIXME ff
      if (Double.compare(Math.min(b1, b3), theta) >= 0) { // bound larger than threshold, start indexing
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
  public int size() {
    return size;
  }

  @Override
  public String toString() {
    return "StreamingL2APIndex [idx=" + idx + ", resList=" + resList + ", ps=" + ps + "]";
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
      return new Iterator<L2APPostingEntry>() {
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
          assert (i == 0); // removal always happens at the head
          ids.popLong();
          weights.popDouble();
          magnitudes.popDouble();
        }
      };
    }
  }
}