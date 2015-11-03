package sssj.index.minibatch;

import static sssj.util.Commons.forgettingFactor;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.util.Iterator;
import java.util.Map;

import sssj.index.AbstractIndex;
import sssj.index.PostingEntry;
import sssj.index.minibatch.component.PostingList;
import sssj.io.Vector;

import com.google.common.primitives.Doubles;

public class InvertedIndex extends AbstractIndex {
  private Int2ReferenceMap<PostingList> idx = new Int2ReferenceOpenHashMap<>();
  private final double theta;
  private final double lambda;

  public InvertedIndex(double theta, double lambda) {
    this.theta = theta;
    this.lambda = lambda;
  }

  @Override
  public Map<Long, Double> queryWith(final Vector v, boolean addToIndex) {
    Long2DoubleOpenHashMap accumulator = new Long2DoubleOpenHashMap(size);
    for (Entry e : v.int2DoubleEntrySet()) {
      final int dimension = e.getIntKey();
      final double queryWeight = e.getDoubleValue();
      PostingList list;
      if ((list = idx.get(dimension)) != null) {
        for (PostingEntry pe : list) {
          numPostingEntries++;
          final long targetID = pe.getID();
          final double targetWeight = pe.getWeight();
          final double additionalSimilarity = queryWeight * targetWeight;
          accumulator.addTo(targetID, additionalSimilarity);
        }
      }
      if (addToIndex) {
        if (list == null) {
          list = new PostingList();
          idx.put(dimension, list);
        }
        list.add(v.timestamp(), queryWeight);
        size++;
      }
    }
    numCandidates += accumulator.size();
    numSimilarities = numCandidates;

    // add forgetting factor e^(-lambda*delta_T) and filter candidates < theta
    for (Iterator<Long2DoubleMap.Entry> it = accumulator.long2DoubleEntrySet().iterator(); it.hasNext();) {
      Long2DoubleMap.Entry e = it.next();
      final long deltaT = v.timestamp() - e.getLongKey();
      e.setValue(e.getDoubleValue() * forgettingFactor(lambda, deltaT));
      if (Doubles.compare(e.getDoubleValue(), theta) < 0)
        it.remove();
    }
    
    numMatches += accumulator.size();
    return accumulator;
  }

  @Override
  public String toString() {
    return "InvertedIndex [idx=" + idx + "]";
  }
}
