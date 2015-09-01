package sssj;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import com.google.common.base.Preconditions;
import com.google.common.collect.ForwardingTable;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;

public class Commons {
  static final double DEFAULT_THETA = 0.5;
  static final double DEFAULT_LAMBDA = 1;
  static final int DEFAULT_REPORT_PERIOD = 10_000;

  public static double tau(double theta, double lambda) {
    Preconditions.checkArgument(theta > 0 && theta < 1);
    Preconditions.checkArgument(lambda > 0);
    double tau = 1 / lambda * Math.log(1 / theta);
    return tau;
  }

  public static double forgetFactor(double lambda, long deltaT) {
    return Math.exp(-lambda * deltaT);
  }

  public static class BatchResult extends ForwardingTable<Long, Long, Double> {
    private final Table<Long, Long, Double> delegate = TreeBasedTable.create();

    @Override
    protected Table<Long, Long, Double> delegate() {
      return delegate;
    }
  }

  public static enum IndexType {
    INVERTED, ALL_PAIRS, L2AP;
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
}
