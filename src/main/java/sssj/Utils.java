package sssj;

import com.google.common.base.Preconditions;

public class Utils {

  public static double computeTau(double lambda, double theta) {
    Preconditions.checkArgument(lambda > 0);
    Preconditions.checkArgument(theta > 0 && theta < 1);
    double tau = 1 / lambda * Math.log(1 / theta);
    return tau;
  }
}
