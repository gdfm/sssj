package sssj;

import java.io.BufferedReader;
import java.util.Map;

import sssj.io.StreamReader;

import com.github.gdfm.shobaidogu.IOUtils;

public class SSSJ {

  public static void main(String[] args) throws Exception {
    System.out.println("RUN!");
    String filename = args[0];
    final double theta = 0.03;
    BufferedReader reader = IOUtils.getBufferedReader(filename);
    StreamReader stream = new StreamReader(reader);
    InvertedIndex index = new InvertedIndex(theta);
    ResidualList residual = new ResidualList();

    long currentTimestamp = -1, previousTimestamp = -1;
    for (Vector v : stream) {
      previousTimestamp = currentTimestamp;
      currentTimestamp = v.timestamp();

      Map<Long, Double> results = index.queryWith(v);
      if (!results.isEmpty())
        System.out.println(v.timestamp() + ": " + results);

      Vector r = index.addVector(v);
      residual.add(r);
    }
  }
}
