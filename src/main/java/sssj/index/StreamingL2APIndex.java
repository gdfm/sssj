package sssj.index;

import static sssj.base.Commons.*;
import it.unimi.dsi.fastutil.BidirectionalIterator;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.math3.util.FastMath;

import sssj.base.CircularBuffer;
import sssj.base.Commons;
import sssj.base.Commons.ResidualList;
import sssj.base.Vector;
import sssj.index.L2APIndex.L2APPostingEntry;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;

public class StreamingL2APIndex implements Index {
// private static final Logger log = LoggerFactory.getLogger(StreamingL2APIndex.class);
  private final Int2ReferenceMap<StreamingL2APPostingList> idx = new Int2ReferenceOpenHashMap<>();
  private final ResidualList resList = new ResidualList();
  private final Long2DoubleOpenHashMap ps = new Long2DoubleOpenHashMap();
  private final Long2DoubleOpenHashMap accumulator = new Long2DoubleOpenHashMap();
  private final Long2DoubleOpenHashMap matches = new Long2DoubleOpenHashMap();
  private final double theta;
  private final double lambda;
  private final double tau;
  private final Vector maxVector; // \hat{y} = \hat{c_w}
  private int size = 0;

  public StreamingL2APIndex(double theta, double lambda) {
    this.theta = theta;
    this.lambda = lambda;
    this.tau = tau(theta, lambda);
    this.maxVector = new Vector();
    System.out.println("Tau = " + tau);
    precomputeFFTable(lambda, (int) Math.ceil(tau));
  }

  @Override
  public Map<Long, Double> queryWith(final Vector v, boolean addToIndex) {
    Preconditions.checkArgument(addToIndex == true); // assume addToIndex == true
    accumulator.clear();
    matches.clear();
    Vector updates = maxVector.updateMaxByDimension(v); // TODO use updates for reindexing
    // upper bound on the forgetting factor w.r.t. the maximum vector
    // TODO can be tightened with a deltaT per dimension
    final double maxForgettingFactor = forgettingFactor(lambda, v.timestamp() - maxVector.timestamp());
    // rs3, enhanced remscore bound with addded forgetting factor, for Streaming maxVector is the max vector in the index
    double remscore = Vector.similarity(v, maxVector) * maxForgettingFactor;
    double l2remscore = 1 * maxForgettingFactor, // rs4
    rst = 1, squaredQueryPrefixMagnitude = 1;

    /* candidate generation */
    for (BidirectionalIterator<Entry> vectIter = v.int2DoubleEntrySet().fastIterator(v.int2DoubleEntrySet().last()); vectIter
        .hasPrevious();) { // iterate over v in reverse order
      final Entry e = vectIter.previous();
      final int dimension = e.getIntKey();
      final double queryWeight = e.getDoubleValue();
      final double rscore = Math.min(remscore, l2remscore);
      squaredQueryPrefixMagnitude -= queryWeight * queryWeight;

      StreamingL2APPostingList list;
      if ((list = idx.get(dimension)) != null) {
        for (Iterator<L2APPostingEntry> listIter = list.iterator(); listIter.hasNext();) {
          final L2APPostingEntry pe = listIter.next();
          final long targetID = pe.getID();

          // time filtering
          final long deltaT = v.timestamp() - targetID;
          if (Doubles.compare(deltaT, tau) > 0) {
            listIter.remove();
            size--;
            continue;
          }

          final double ff = forgettingFactor(lambda, deltaT);
          if (accumulator.containsKey(targetID) || Double.compare(rscore, theta) >= 0) {
            final double targetWeight = pe.getWeight(); // y_j
            final double additionalSimilarity = queryWeight * targetWeight * ff; // x_j * y_j * e^(-lambda*deltaT)
            accumulator.addTo(targetID, additionalSimilarity); // A[y] += x_j * y_j * e^(-lambda*deltaT)
            final double l2bound = accumulator.get(targetID) + FastMath.sqrt(squaredQueryPrefixMagnitude)
                * pe.magnitude; // A[y] + ||x'_j|| * ||y'_j||
            // forgetting factor applied directly to the l2bound (not to squaredQueryPrefixMagnitude)
            if (Double.compare(l2bound * ff, theta) < 0) // ( A[y] + ||x'_j|| * ||y'_j|| ) * e^(-lambda*deltaT)
              accumulator.remove(targetID); // prune this candidate (early verification)
          }
          final double targetWeight = pe.getWeight();
          final double additionalSimilarity = queryWeight * targetWeight * ff;
          accumulator.addTo(targetID, additionalSimilarity);
        }
        // forgetting factor applied directly to the bounds (not to rst)
        remscore -= queryWeight * maxVector.get(dimension) * maxForgettingFactor; // rs_3 -= x_j * \hat{c_w} * e^(-lambda*deltaT)
        rst -= queryWeight * queryWeight; // rs_t -= x_j^2
        l2remscore = FastMath.sqrt(rst) * maxForgettingFactor; // rs_4 = sqrt(rs_t) * e^(-lambda*deltaT)
        // FIXME adding should happen here
        // } else {
        // list = new StreamingL2APPostingList();
        // idx.put(dimension, list);
      }
      // list.add(v.timestamp(), queryWeight, );
      // size++;
    }

    /* candidate verification */
    for (Long2DoubleMap.Entry e : accumulator.long2DoubleEntrySet()) {
      final long candidateID = e.getLongKey();
      final long deltaT = v.timestamp() - candidateID;
      final double ff = forgettingFactor(lambda, deltaT);
      if (Double.compare(e.getDoubleValue() + ps.get(candidateID) * ff, theta) < 0) // A[y] = dot(x, y'')
        continue; // l2 pruning
      Vector residual = resList.get(candidateID);
      assert (residual != null);
      double dpscore = e.getDoubleValue() + Math.min(v.maxValue() * residual.size(), residual.maxValue() * v.size());
      if (Double.compare(dpscore * ff, theta) < 0)
        continue; // dpscore, eq. (5) * e^(-lambda*deltaT)

      double score = e.getDoubleValue() + Vector.similarity(v, residual) * ff; // dot(x, y) = A[y] + dot(x, y')
      // score *= ff; // apply forgetting factor // FIXME to avoid double counting ff we are applying it only to Vector.similarity. However we could avoid it
// alltogether in A[y]
      if (Double.compare(score, theta) >= 0) // final check
        matches.put(candidateID, score);
    }

    
    // TODO UP TO HERE
    if (addToIndex) {
      double b1 = 0, bt = 0, b3 = 0, pscore = 0;
      boolean psSaved = false;
      Vector residual = new Vector(v.timestamp());
      for (Entry e : v.int2DoubleEntrySet()) {
        int dimension = e.getIntKey();
        double weight = e.getDoubleValue();

        pscore = Math.min(b1, b3);
        b1 += weight * maxVector.get(dimension);
        bt += weight * weight;
        b3 = FastMath.sqrt(bt);

        if (Double.compare(Math.min(b1, b3), theta) >= 0) {
          if (!psSaved) {
            assert (!ps.containsKey(v.timestamp()));
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
      resList.add(residual);
      // maxVector.updateMaxByDimension(v); // max already udpated
    }

    return matches;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public String toString() {
    return "L2APIndex [idx=" + idx + ", resList=" + resList + ", ps=" + ps + "]";
  }

  static class StreamingL2APPostingList implements Iterable<L2APPostingEntry> {
    private CircularBuffer ids = new CircularBuffer(); // longs
    private CircularBuffer weights = new CircularBuffer(); // doubles
    private CircularBuffer magnitudes = new CircularBuffer(); // doubles

    public void add(long vectorID, double weight, double magnitude) {
      ids.pushLong(vectorID);
      weights.pushDouble(weight);
      magnitudes.pushDouble(magnitude);
    }

    @Override
    public String toString() {
      return "[ids=" + ids + ", weights=" + weights + ", magnitudes=" + magnitudes + "]";
    }

    @Override
    public Iterator<L2APPostingEntry> iterator() {
      return new StreamingPostingListIterator();
    }

    class StreamingPostingListIterator implements Iterator<L2APPostingEntry> {
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
        assert (i == 0); // removals always happen at the head
        ids.popLong();
        weights.popDouble();
        magnitudes.popDouble();
      }
    }
  }
}
