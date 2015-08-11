package sssj;

import it.unimi.dsi.fastutil.longs.Long2DoubleMap;

import java.io.BufferedReader;
import java.util.Map;

import sssj.io.StreamReader;

import com.github.gdfm.shobaidogu.IOUtils;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

public class Baseline {

  public static void main(String[] args) throws Exception {
    System.out.println("Baseline!");
    String filename = args[0];
    final double threshold = 0.03;
    BufferedReader reader = IOUtils.getBufferedReader(filename);
    StreamReader stream = new StreamReader(reader);
    InvertedIndex index = new InvertedIndex(threshold);
    ResidualIndex residual = new ResidualIndex();
    
    long currentTimestamp = -1, previousTimestamp = -1;
    for (Vector v : stream) {
      previousTimestamp = currentTimestamp;
      currentTimestamp = v.getTimestamp();

      Long2DoubleMap matches = index.queryWith(v);
      Map<Long, Double> results = Maps.filterValues(matches, new Predicate<Double>() { // TODO should not be needed
        @Override
        public boolean apply(Double input) {
          return input.compareTo(threshold) >= 0;
        }
      });
      System.out.println(v.getTimestamp() + ": " + results);

      Vector r = index.addVector(v);
      residual.add(r);
    }
  }
}
