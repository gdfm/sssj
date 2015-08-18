package sssj.time;

import org.apache.commons.math3.distribution.PoissonDistribution;

public interface Timeline {
  long nextTimestamp();

  public static class Sequential implements Timeline {
    private long current = 0;

    @Override
    public long nextTimestamp() {
      return current++;
    }
  }

  public static class Poisson implements Timeline {
    private long current = 0;
    private PoissonDistribution p;

    public Poisson(double rate) {
      p = new PoissonDistribution(rate);
    }

    @Override
    public long nextTimestamp() {
      current += p.sample();
      return current;
    }
  }
}