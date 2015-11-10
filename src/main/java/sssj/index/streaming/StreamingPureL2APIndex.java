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

import java.util.Map;

import org.apache.commons.math3.util.FastMath;

import sssj.index.AbstractIndex;
import sssj.index.L2APPostingEntry;
import sssj.index.streaming.component.StreamingL2APPostingList;
import sssj.index.streaming.component.StreamingL2APPostingList.StreamingL2APPostingListIterator;
import sssj.index.streaming.component.StreamingResiduals;
import sssj.io.Vector;

import com.google.common.primitives.Doubles;

public class StreamingPureL2APIndex extends AbstractIndex {
  private final Int2ReferenceMap<StreamingL2APPostingList> idx = new Int2ReferenceOpenHashMap<>();
  private final StreamingResiduals residuals = new StreamingResiduals();
  private final Long2DoubleLinkedOpenHashMap ps = new Long2DoubleLinkedOpenHashMap();
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
      Vector residual = addToIndex(v);
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
      final double rscore = l2remscore; // forgetting factor applied directly to the l2prefix bound
      squaredQueryPrefixMagnitude -= queryWeight * queryWeight;

      StreamingL2APPostingList list;
      if ((list = idx.get(dimension)) != null) {
        for (StreamingL2APPostingListIterator listIter = list.reverseIterator(); listIter.hasPrevious();) {
          numPostingEntries++;
          final L2APPostingEntry pe = listIter.previous();
          final long targetID = pe.id(); // y

          // time filtering
          final long deltaT = v.timestamp() - targetID;
          if (Doubles.compare(deltaT, tau) > 0) {
            listIter.next(); // back off one position
            numPostingEntries--; // do not count the last entry
            size -= listIter.nextIndex(); // update size before cutting
            listIter.cutHead(); // prune the head
            break;
          }

          final double ff = forgettingFactor(lambda, deltaT);
          if (accumulator.containsKey(targetID) || Double.compare(rscore, theta) >= 0) {
            final double targetWeight = pe.weight(); // y_j
            final double additionalSimilarity = queryWeight * targetWeight; // x_j * y_j
            accumulator.addTo(targetID, additionalSimilarity); // A[y] += x_j * y_j
            final double l2bound = accumulator.get(targetID) + FastMath.sqrt(squaredQueryPrefixMagnitude)
                * pe.magnitude(); // A[y] + ||x'_j|| * ||y'_j||
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
      final long candidateID = e.getLongKey();
      final long deltaT = v.timestamp() - candidateID;
      if (deltaT > tau) // time pruning
        continue;
      final double ff = forgettingFactor(lambda, deltaT);
      if (Double.compare((e.getDoubleValue() + ps.get(candidateID)) * ff, theta) < 0) // A[y] = dot(x, y")
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

  private void prunePS(long lowWatermark) {
    ObjectIterator<Long2DoubleMap.Entry> it = ps.long2DoubleEntrySet().fastIterator();
    while (it.hasNext() && it.next().getLongKey() < lowWatermark) {
      it.remove();
    }
  }

  @Override
  public String toString() {
    return "StreamingPureL2APIndex [idx=" + idx + ", residuals=" + residuals + ", ps=" + ps + "]";
  }
}