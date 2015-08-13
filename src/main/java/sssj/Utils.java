package sssj;

import com.google.common.base.Preconditions;
import com.google.common.collect.ForwardingTable;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class Utils {

  public static double computeTau(double theta, double lambda) {
    Preconditions.checkArgument(theta > 0 && theta < 1);
    Preconditions.checkArgument(lambda > 0);
    double tau = 1 / lambda * Math.log(1 / theta);
    return tau;
  }

  public static class BatchResult extends ForwardingTable<Long, Long, Double> {
    private final HashBasedTable<Long, Long, Double> delegate = HashBasedTable.create();

    @Override
    protected Table<Long, Long, Double> delegate() {
      return delegate;
    }
  }

  public static enum IndexType {
    INVERTED, ALL_PAIRS, L2AP;
  }
}
