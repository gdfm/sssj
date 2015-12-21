package sssj.index.minibatch;

import static sssj.util.Commons.forgettingFactor;
import it.unimi.dsi.fastutil.BidirectionalIterator;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.util.Map;

import org.apache.commons.math3.util.FastMath;

import sssj.index.AbstractIndex;
import sssj.index.L2APPostingEntry;
import sssj.index.minibatch.component.L2APPostingList;
import sssj.index.minibatch.component.Residuals;
import sssj.io.Vector;

public class L2Index extends AbstractIndex {
  private final Int2ReferenceMap<L2APPostingList> idx = new Int2ReferenceOpenHashMap<>();
  private final Residuals residuals = new Residuals();
  private final Long2DoubleOpenHashMap ps = new Long2DoubleOpenHashMap();
  private final double theta;
  private final double lambda;

  public L2Index(double theta, double lambda) {
    this.theta = theta;
    this.lambda = lambda;
  }

  @Override
  public Map<Long, Double> queryWith(final Vector v, final boolean addToIndex) {
    /* candidate generation */
    Long2DoubleOpenHashMap accumulator = generateCandidates(v);
    /* candidate verification */
    Long2DoubleOpenHashMap matches = verifyCandidates(v, accumulator);
    /* index building */
    if (addToIndex) {
      Vector residual = addToIndex(v);
      residuals.add(residual);
    }
    return matches;
  }

  private final Long2DoubleOpenHashMap generateCandidates(final Vector v) {
    final Long2DoubleOpenHashMap accumulator = new Long2DoubleOpenHashMap();
    double l2remscore = 1, // rs4
    rst = 1, squaredQueryPrefixMagnitude = 1;

    for (BidirectionalIterator<Entry> vecIter = v.int2DoubleEntrySet().fastIterator(v.int2DoubleEntrySet().last()); vecIter
        .hasPrevious();) { // iterate over v in reverse order
      final Entry e = vecIter.previous();
      final int dimension = e.getIntKey();
      final double queryWeight = e.getDoubleValue(); // x_j
      final double rscore = l2remscore;
      squaredQueryPrefixMagnitude -= queryWeight * queryWeight;

      L2APPostingList list;
      if ((list = idx.get(dimension)) != null) {
        for (L2APPostingEntry pe : list) {
          numPostingEntries++;
          final long targetID = pe.id(); // y
          if (accumulator.containsKey(targetID) || Double.compare(rscore, theta) >= 0) {
            final double targetWeight = pe.weight(); // y_j
            final double additionalSimilarity = queryWeight * targetWeight; // x_j * y_j
            accumulator.addTo(targetID, additionalSimilarity); // A[y] += x_j * y_j
            final double l2bound = accumulator.get(targetID) + FastMath.sqrt(squaredQueryPrefixMagnitude)
                * pe.magnitude(); // A[y] + ||x'_j|| * ||y'_j||
            if (Double.compare(l2bound, theta) < 0)
              accumulator.remove(targetID); // prune this candidate (early verification)
          }
        }
        rst -= queryWeight * queryWeight; // rs_t -= x_j^2
        l2remscore = FastMath.sqrt(rst); // rs_4 = sqrt(rs_t)
      }
    }
    numCandidates += accumulator.size();
    return accumulator;
  }

  private final Long2DoubleOpenHashMap verifyCandidates(final Vector v, Long2DoubleOpenHashMap accumulator) {
    Long2DoubleOpenHashMap matches = new Long2DoubleOpenHashMap();
    for (Long2DoubleMap.Entry e : accumulator.long2DoubleEntrySet()) {
      final long candidateID = e.getLongKey();
      if (Double.compare(e.getDoubleValue() + ps.get(candidateID), theta) < 0) // A[y] = dot(x, y'')
        continue; // l2 pruning
      Vector residual = residuals.get(candidateID);
      assert (residual != null);
      final double ds1 = e.getDoubleValue()
          + Math.min(v.maxValue() * residual.sumValues(), residual.maxValue() * v.sumValues());
      if (Double.compare(ds1, theta) < 0)
        continue; // dpscore, eq. (5)
      final double sz2 = e.getDoubleValue() + Math.min(v.maxValue() * residual.size(), residual.maxValue() * v.size());
      if (Double.compare(sz2, theta) < 0)
        continue; // dpscore, eq. (9)

      final long deltaT = v.timestamp() - candidateID;
      double score = e.getDoubleValue() + Vector.similarity(v, residual); // dot(x, y) = A[y] + dot(x, y')
      score *= forgettingFactor(lambda, deltaT); // apply forgetting factor
      numSimilarities++;
      if (Double.compare(score, theta) >= 0) // final check
        matches.put(candidateID, score);
    }
    numMatches += matches.size();
    return matches;
  }

  private final Vector addToIndex(final Vector v) {
    double bt = 0, b3 = 0, pscore = 0;
    boolean psSaved = false;
    Vector residual = new Vector(v.timestamp());

    for (Entry e : v.int2DoubleEntrySet()) {
      int dimension = e.getIntKey();
      double weight = e.getDoubleValue();

      pscore = b3;
      bt += weight * weight;
      b3 = FastMath.sqrt(bt);

      if (Double.compare(b3, theta) >= 0) {
        if (!psSaved) {
          assert (!ps.containsKey(v.timestamp()));
          ps.put(v.timestamp(), pscore);
          psSaved = true;
        }
        L2APPostingList list;
        if ((list = idx.get(dimension)) == null) {
          list = new L2APPostingList();
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

  @Override
  public String toString() {
    return "L2Index [idx=" + idx + ", residuals=" + residuals + ", ps=" + ps + "]";
  }
}