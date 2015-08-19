package sssj;

import com.google.common.base.Preconditions;
import com.google.common.collect.ForwardingTable;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;

public class Utils {

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
}
