package sssj;

import java.io.BufferedReader;
import java.util.Map;

import sssj.index.InvertedIndex;
import sssj.index.ResidualList;
import sssj.io.Format;
import sssj.io.VectorStreamReader;

import com.github.gdfm.shobaidogu.IOUtils;

public class SSSJ {

  public static void main(String[] args) throws Exception {
    System.out.println("RUN!");
    String filename = args[0];
    final double theta = 0.03;
    final double lambda = 0.1;
    BufferedReader reader = IOUtils.getBufferedReader(filename);
    VectorStreamReader stream = new VectorStreamReader(reader, Format.SSSJ);
    InvertedIndex index = new InvertedIndex(theta, lambda);
    ResidualList residual = new ResidualList();

    // TODO first update MAX, then query, then index
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
