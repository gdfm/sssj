package sssj.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.math3.util.FastMath;

import com.google.common.base.Preconditions;

public class Commons {
  public static final double DEFAULT_THETA = 0.5;
  public static final double DEFAULT_LAMBDA = 0.01;
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
    assert (FF != null);
    if (deltaT >= FF.length)
      return FastMath.exp(-lambda * deltaT);
    assert (deltaT >= 0 && deltaT < FF.length);
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
}
