package sssj.index;

import it.unimi.dsi.fastutil.BidirectionalIterator;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.util.Map;

import sssj.Utils;
import sssj.Vector;
import sssj.index.InvertedIndex.PostingEntry;
import sssj.index.InvertedIndex.PostingList;

public class APIndex implements Index {
  private Int2ReferenceMap<PostingList> idx = new Int2ReferenceOpenHashMap<>();
  private ResidualList resList = new ResidualList();
  private int size = 0;
  private final double theta;
  private final double lambda;
  private final Vector maxVector;

  public APIndex(double theta, double lambda, Vector maxVector) {
    this.theta = theta;
    this.lambda = lambda;
    this.maxVector = maxVector;
  }

  @Override
  public Map<Long, Double> queryWith(Vector v) {
    Long2DoubleOpenHashMap matches = new Long2DoubleOpenHashMap();
    Long2DoubleOpenHashMap accumulator = new Long2DoubleOpenHashMap(size);
    // int minSize = theta / rw_x; //TODO possibly size filtering (need to sort dataset by max row weight rw_x)
    double remscore = Vector.similarity(v, maxVector);

    /* candidate generation */
    for (BidirectionalIterator<Entry> it = v.int2DoubleEntrySet().fastIterator(v.int2DoubleEntrySet().last()); it
        .hasPrevious();) { // iterate over v in reverse order
      Entry e = it.previous();
      int dimension = e.getIntKey();
      double queryWeight = e.getDoubleValue(); // x_j

      if (idx.containsKey(dimension)) {
        PostingList list = idx.get(dimension);
        // TODO possibly size filtering: remove entries from the posting list with |y| < minsize (need to save size in the posting list)
        for (PostingEntry pe : list) {
          long targetID = pe.getLongKey(); // y
          if (accumulator.containsKey(targetID) || Double.compare(remscore, theta) >= 0) {
            double targetWeight = pe.getDoubleValue(); // y_j
            double additionalSimilarity = queryWeight * targetWeight; // x_j * y_j
            // TODO add e^(-lambda*delta_t)
            accumulator.addTo(targetID, additionalSimilarity); // A[y] += x_j * y_j
          }
        }
      }
      remscore -= queryWeight * maxVector.get(dimension);
    }

    /* candidate verification */
    for (Long2DoubleMap.Entry e : accumulator.long2DoubleEntrySet()) {
      // TODO possibly use size filtering (sz_3)
      long candidateID = e.getLongKey();
      Vector candidateResidual = resList.get(candidateID);
      assert (candidateResidual != null);
      double score = e.getDoubleValue() + Vector.similarity(v, candidateResidual); // A[y] + dot(y',x)
      long deltaT = v.timestamp() - candidateID;
      score *= Utils.forget(lambda, deltaT); // TODO move into similarity and index e^(-lambda*delta_T)
      if (Double.compare(score, theta) >= 0) // final check
        matches.put(candidateID, score);
    }
    return matches;
  }

  @Override
  public Vector addVector(Vector v) {
    size++;
    double pscore = 0;
    Vector residual = new Vector(v.timestamp());
    for (Entry e : v.int2DoubleEntrySet()) {
      int dimension = e.getIntKey();
      double weight = e.getDoubleValue();
      pscore += weight * maxVector.get(dimension);
      if (Double.compare(pscore, theta) >= 0) {
        if (!idx.containsKey(dimension))
          idx.put(dimension, new PostingList());
        idx.get(dimension).add(v.timestamp(), weight);
        // v.remove(dimension);
      } else {
        residual.put(dimension, weight);
      }
    }
    resList.add(residual);
    return residual;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public String toString() {
    return "APIndex [idx=" + idx + ", resList=" + resList + "]";
  }
}
