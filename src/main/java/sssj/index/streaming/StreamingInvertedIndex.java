package sssj.index.streaming;

import static sssj.util.Commons.*;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.util.Iterator;
import java.util.Map;

import sssj.index.AbstractIndex;
import sssj.index.PostingEntry;
import sssj.index.streaming.component.StreamingPostingList;
import sssj.index.streaming.component.StreamingPostingList.StreamingPostingListIterator;
import sssj.io.Vector;

import com.google.common.primitives.Doubles;

public class StreamingInvertedIndex extends AbstractIndex {
  private Int2ReferenceMap<StreamingPostingList> idx = new Int2ReferenceOpenHashMap<>();
  private final Long2DoubleOpenHashMap accumulator = new Long2DoubleOpenHashMap();
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
        for (StreamingPostingListIterator listIter = list.reverseIterator(); listIter.hasPrevious();) {
          final PostingEntry pe = listIter.previous();
          final long targetID = pe.getID();

          // time filtering
          final long deltaT = v.timestamp() - targetID;
          if (Doubles.compare(deltaT, tau) > 0) {
            listIter.next(); // back off one position
            size -= listIter.nextIndex(); // update index size before cutting
            listIter.cutHead(); // prune the head
            break;
          }
          numPostingEntries++;

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
    numCandidates += accumulator.size();
    numSimilarities = numCandidates;

    // filter candidates < theta
    for (Iterator<Long2DoubleMap.Entry> it = accumulator.long2DoubleEntrySet().iterator(); it.hasNext();)
      if (Doubles.compare(it.next().getDoubleValue(), theta) < 0)
        it.remove();

    numMatches += accumulator.size();
    return accumulator;
  }

  @Override
  public String toString() {
    return "StreamingInvertedIndex [idx=" + idx + "]";
  }
}
