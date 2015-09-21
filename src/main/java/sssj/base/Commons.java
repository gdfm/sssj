package sssj.base;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import org.apache.commons.math3.util.FastMath;

import com.google.common.base.Preconditions;
import com.google.common.collect.ForwardingTable;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;

public class Commons {
  public static final double DEFAULT_THETA = 0.5;
  public static final double DEFAULT_LAMBDA = 0.1;
  public static final int DEFAULT_REPORT_PERIOD = 10_000;

  private static double[] FF; // precomputed values for the forgetting factor

  public static double tau(final double theta, final double lambda) {
    Preconditions.checkArgument(theta > 0 && theta < 1);
    Preconditions.checkArgument(lambda > 0);
    double tau = 1 / lambda * Math.log(1 / theta);
    return tau;
  }

  public static void precomputeFFTable(final double lambda, final int tau) {
    FF = new double[tau];
    for (int i = 0; i < tau; i++)
      FF[i] = FastMath.exp(-lambda * i);
  }

  public static double forgettingFactor(final double lambda, final long deltaT) {
    assert (FF != null && deltaT >= 0 && deltaT < FF.length);
    return FF[(int) deltaT];
  }

  public static String formatMap(final Map<Long, Double> map) {
    StringBuilder sb = new StringBuilder();
    sb.append('{');
    Iterator<Entry<Long, Double>> iter = map.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<Long, Double> entry = iter.next();
      sb.append(entry.getKey()).append(':').append(String.format("%.5f", entry.getValue()));
      if (iter.hasNext())
        sb.append(", ");
    }
    sb.append('}');
    return sb.toString();
  }

  public static class BatchResult extends ForwardingTable<Long, Long, Double> {
    private final Table<Long, Long, Double> delegate = TreeBasedTable.create();

    @Override
    protected Table<Long, Long, Double> delegate() {
      return delegate;
    }
  }

  public static class ResidualList implements Iterable<Vector> {
    private Queue<Vector> queue = new LinkedList<>();

    public void add(Vector residual) {
      queue.add(residual);
    }

    @Override
    public Iterator<Vector> iterator() {
      return queue.iterator();
    }

    @Override
    public String toString() {
      return "ResidualList = [" + queue + "]";
    }

    public Vector get(long candidateID) {
      for (Vector v : queue)
        if (candidateID == v.timestamp())
          return v;
      return null;
    }
  }

  public static enum IndexType {
    INVERTED(false), ALLPAIRS(true), L2AP(true);

    IndexType(boolean needsMax) {
      this.needsMax = needsMax;
    }

    public boolean needsMax() {
      return needsMax;
    }

    private final boolean needsMax;
  }
}
