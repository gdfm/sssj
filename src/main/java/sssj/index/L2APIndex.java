package sssj.index;

import java.util.Map;

import sssj.Vector;

import com.google.common.collect.ForwardingTable;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class L2APIndex implements Index {
  //  private final double theta;
  private InvertedIndex index;

  public L2APIndex(double theta) {
    //    this.theta = theta;
    this.index = new InvertedIndex(theta);
  }



  @Override
  public Map<Long, Double> queryWith(Vector v) {
    return index.queryWith(v);
  }

  @Override
  public Vector addVector(Vector v) {
    return index.addVector(v);
  }
}