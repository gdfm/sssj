package sssj.index.streaming;

import static sssj.util.Commons.*;
import it.unimi.dsi.fastutil.BidirectionalIterator;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleLinkedOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.util.FastMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sssj.index.AbstractIndex;
import sssj.index.L2APPostingEntry;
import sssj.index.minibatch.component.MaxVector;
import sssj.index.streaming.component.StreamingL2APPostingList;
import sssj.index.streaming.component.StreamingL2APPostingList.StreamingL2APPostingListIterator;
import sssj.index.streaming.component.StreamingMaxVector;
import sssj.index.streaming.component.StreamingResiduals;
import sssj.io.Vector;

import com.google.common.primitives.Doubles;

public class StreamingL2APIndex extends AbstractIndex {
  private static final Logger log = LoggerFactory.getLogger(StreamingL2APIndex.class);
  private final Int2ReferenceMap<StreamingL2APPostingList> idx = new Int2ReferenceOpenHashMap<>();
  private final StreamingResiduals residuals = new StreamingResiduals();
  private final Long2DoubleLinkedOpenHashMap ps = new Long2DoubleLinkedOpenHashMap();
  private final Long2DoubleOpenHashMap accumulator = new Long2DoubleOpenHashMap();
  private final Long2DoubleOpenHashMap matches = new Long2DoubleOpenHashMap();
  private final MaxVector maxVector; // c_w
  private final StreamingMaxVector maxVectorInIndex; // \hat{c_w}
  private final double theta;
  private final double lambda;
  private final double tau;

  private long numReindexing, maxUpdates;

  public StreamingL2APIndex(double theta, double lambda) {
    this.theta = theta;
    this.lambda = lambda;
    this.maxVector = new MaxVector();
    this.maxVectorInIndex = new StreamingMaxVector(lambda);
    this.tau = tau(theta, lambda);
    System.out.println("Tau = " + tau);
    precomputeFFTable(lambda, (int) Math.ceil(tau));
  }

  @Override
  public Map<Long, Double> queryWith(final Vector v, final boolean addToIndex) {
    accumulator.clear();
    matches.clear();
    maxVectorInIndex.updateMaxByDimensionFF(v);
    /* reindexing */
    Vector updates = maxVector.updateMaxByDimension(v);
    if (updates.size() > 0)
      reindex(updates);
    /* candidate generation */
    generateCandidates(v);
    /* candidate verification */
    verifyCandidates(v);
    /* index building */
    if (addToIndex) {
      Vector residual = addToIndex(v);
      residuals.add(residual);
    }
    return matches;
  }

  private final void reindex(Vector updates) {
    List<Vector> newRes = new LinkedList<>();
    for (Iterator<Vector> it = residuals.iterator(); it.hasNext();) {
      final Vector r = it.next();
      final double simDelta = Vector.similarity(updates, r);
      if (simDelta > 0) {
        final double pscore = ps.get(r.timestamp());
        if (Doubles.compare(pscore + simDelta, theta) >= 0) {
          final Vector newResidual = addToIndex(r);
          newRes.add(newResidual);
        }
      }
    }
    for (Vector r : newRes)
      residuals.add(r);
  }

  private final void generateCandidates(final Vector v) {
    double remscore = maxVectorInIndex.simimarityFF(v); // rs3, enhanced remscore bound with addded forgetting factor per dimension
    double l2remscore = 1, // rs4
    rst = 1, squaredQueryPrefixMagnitude = 1;

    for (BidirectionalIterator<Entry> vecIter = v.int2DoubleEntrySet().fastIterator(v.int2DoubleEntrySet().last()); vecIter
        .hasPrevious();) { // iterate over v in reverse order
      final Entry e = vecIter.previous();
      final int dimension = e.getIntKey();
      final double queryWeight = e.getDoubleValue(); // x_j
      squaredQueryPrefixMagnitude -= queryWeight * queryWeight;

      StreamingL2APPostingList list;
      if ((list = idx.get(dimension)) != null) {
        for (StreamingL2APPostingListIterator listIter = list.iterator(); listIter.hasNext();) {
          final L2APPostingEntry pe = listIter.next();
          final long targetID = pe.id(); // y

          // time filtering
          numPostingEntries++;
          final long deltaT = v.timestamp() - targetID;
          if (Doubles.compare(deltaT, tau) > 0) {
            listIter.remove();
            size--;
            continue;
          }

          final double ff = forgettingFactor(lambda, deltaT);
          // forgetting factor applied directly to the l2prefix bounds
          final double rscore = Math.min(remscore, l2remscore * ff);
          if (accumulator.containsKey(targetID) || Double.compare(rscore, theta) >= 0) {
            final double targetWeight = pe.weight(); // y_j
            final double additionalSimilarity = queryWeight * targetWeight; // x_j * y_j
            accumulator.addTo(targetID, additionalSimilarity); // A[y] += x_j * y_j
            final double l2bound = accumulator.get(targetID) + FastMath.sqrt(squaredQueryPrefixMagnitude)
                * pe.magnitude(); // A[y] + ||x'_j|| * ||y'_j||
            // forgetting factor applied directly to the l2bound
            if (Double.compare(l2bound * ff, theta) < 0)
              accumulator.remove(targetID); // prune this candidate (early verification)
          }
        }
        final double dimFF = maxVectorInIndex.dimensionFF(dimension, v.timestamp());
        remscore -= queryWeight * maxVectorInIndex.get(dimension) * dimFF; // rs_3 -= x_j * \hat{c_w}
        rst -= queryWeight * queryWeight; // rs_t -= x_j^2
        l2remscore = FastMath.sqrt(rst); // rs_4 = sqrt(rs_t)
      }
    }
    numCandidates += accumulator.size();
  }

  private final void verifyCandidates(final Vector v) {
    for (Long2DoubleMap.Entry e : accumulator.long2DoubleEntrySet()) {
      final long candidateID = e.getLongKey();
      final long deltaT = v.timestamp() - candidateID;
      if (deltaT > tau) // time pruning
        continue;
      final double ff = forgettingFactor(lambda, deltaT);
      if (Double.compare((e.getDoubleValue() + ps.get(candidateID)) * ff, theta) < 0) // A[y] = dot(x, y'')
        continue; // l2 pruning

      final long lowWatermark = (long) Math.floor(v.timestamp() - tau);
      final Vector residual = residuals.getAndPrune(candidateID, lowWatermark);
      prunePS(lowWatermark);
      assert (residual != null) : "Residual not found. ID=" + v.timestamp() + " candidateID=" + candidateID;
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
    numMatches += matches.size();
  }

  private final Vector addToIndex(final Vector v) {
    double b1 = 0, bt = 0, b3 = 0, pscore = 0;
    boolean psSaved = false;
    final Vector residual = new Vector(v.timestamp());

    for (Entry e : v.int2DoubleEntrySet()) {
      final int dimension = e.getIntKey();
      final double weight = e.getDoubleValue();
      pscore = Math.min(b1, b3);
      b1 += weight * maxVector.get(dimension);
      bt += weight * weight;
      b3 = FastMath.sqrt(bt);

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
        list.add(v.timestamp(), weight, pscore);
        size++;
      } else {
        residual.put(dimension, weight);
      }
    }
    return residual;
  }

  private void prunePS(long lowWatermark) {
    ObjectIterator<Long2DoubleMap.Entry> it = ps.long2DoubleEntrySet().fastIterator();
    while (it.hasNext() && it.next().getLongKey() < lowWatermark) {
      it.remove();
    }
  }

  @Override
  public String toString() {
    return "StreamingL2APIndex [idx=" + idx + ", residuals=" + residuals + ", ps=" + ps + "]";
  }
}