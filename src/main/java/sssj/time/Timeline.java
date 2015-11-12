package sssj.time;

import org.apache.commons.math3.distribution.ExponentialDistribution;

public interface Timeline {
  long nextTimestamp();

  public static class Sequential implements Timeline {
    private long current = 0;

    @Override
    public long nextTimestamp() {
      return current++;
    }

    @Override
    public String toString() {
      return "Sequential";
    }
  }

  public static class Poisson implements Timeline {
    private long current = 0;
    private ExponentialDistribution p;

    public Poisson(double rate) {
      // exponential interarrival times with mean = 1/lambda
      p = new ExponentialDistribution(1 / rate);
    }

    @Override
    public long nextTimestamp() {
      current += Math.max(1, p.sample()); // ensure unique timestamps
      return current;
    }

    @Override
    public String toString() {
      return "Poisson(" + 1.0 / p.getMean() + ")";
    }
  }
}