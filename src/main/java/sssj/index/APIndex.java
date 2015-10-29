package sssj.index;

import it.unimi.dsi.fastutil.BidirectionalIterator;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.util.Map;

import sssj.base.Commons;
import sssj.base.Residuals;
import sssj.base.Vector;
import sssj.index.InvertedIndex.PostingEntry;
import sssj.index.InvertedIndex.PostingList;

public class APIndex extends AbstractIndex {
  private Int2ReferenceMap<PostingList> idx = new Int2ReferenceOpenHashMap<>();
  private Residuals residuals = new Residuals();
  private final double theta;
  private final double lambda;
  private final Vector maxVector;

  public APIndex(double theta, double lambda, Vector maxVector) {
    this.theta = theta;
    this.lambda = lambda;
    this.maxVector = maxVector;
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

  private Long2DoubleOpenHashMap generateCandidates(final Vector v) {
    Long2DoubleOpenHashMap accumulator = new Long2DoubleOpenHashMap();
    // int minSize = theta / rw_x; //TODO possibly size filtering (need to sort dataset by max row weight rw_x)
    double remscore = Vector.similarity(v, maxVector);

    /* candidate generation */
    for (BidirectionalIterator<Entry> it = v.int2DoubleEntrySet().fastIterator(v.int2DoubleEntrySet().last()); it
        .hasPrevious();) { // iterate over v in reverse order
      Entry e = it.previous();
      int dimension = e.getIntKey();
      double queryWeight = e.getDoubleValue(); // x_j

      PostingList list;
      if ((list = idx.get(dimension)) != null) {
        // TODO possibly size filtering: remove entries from the posting list with |y| < minsize (need to save size in the posting list)
        for (PostingEntry pe : list) {
          numPostingEntries++;
          final long targetID = pe.getID(); // y
          if (accumulator.containsKey(targetID) || Double.compare(remscore, theta) >= 0) {
            final double targetWeight = pe.getWeight(); // y_j
            final double additionalSimilarity = queryWeight * targetWeight; // x_j * y_j
            accumulator.addTo(targetID, additionalSimilarity); // A[y] += x_j * y_j
          }
        }
      }
      remscore -= queryWeight * maxVector.get(dimension);
    }
    numCandidates += accumulator.size();
    return accumulator;
  }

  private Long2DoubleOpenHashMap verifyCandidates(final Vector v, Long2DoubleOpenHashMap accumulator) {
    Long2DoubleOpenHashMap matches = new Long2DoubleOpenHashMap();
    for (Long2DoubleMap.Entry e : accumulator.long2DoubleEntrySet()) {
      // TODO possibly use size filtering (sz_3)
      long candidateID = e.getLongKey();
      Vector candidateResidual = residuals.get(candidateID);
      assert (candidateResidual != null);
      double score = e.getDoubleValue() + Vector.similarity(v, candidateResidual); // A[y] + dot(y',x)
      long deltaT = v.timestamp() - candidateID;
      score *= Commons.forgettingFactor(lambda, deltaT); // apply forgetting factor
      numSimilarities++;
      if (Double.compare(score, theta) >= 0) // final check
        matches.put(candidateID, score);
    }
    return matches;
  }

  private Vector addToIndex(final Vector v) {
    double pscore = 0;
    Vector residual = new Vector(v.timestamp());
    for (Entry e : v.int2DoubleEntrySet()) {
      int dimension = e.getIntKey();
      double weight = e.getDoubleValue();
      pscore += weight * maxVector.get(dimension);
      if (Double.compare(pscore, theta) >= 0) {
        PostingList list;
        if ((list = idx.get(dimension)) == null) {
          list = new PostingList();
          idx.put(dimension, list);
        }
        list.add(v.timestamp(), weight);
        size++;
        // v.remove(dimension);
      } else {
        residual.put(dimension, weight);
      }
    }
    return residual;
  }

  @Override
  public String toString() {
    return "APIndex [idx=" + idx + ", residuals=" + residuals + "]";
  }
}
