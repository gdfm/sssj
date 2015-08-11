package sssj;

import java.util.Iterator;

import com.google.common.collect.ForwardingTable;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class L2APIndex {

  public L2APResult buildIndex(Vector maxVector, Iterator<Vector> firstHalf) {
    // TODO Auto-generated method stub
    L2APResult result = new L2APResult();
    result.put(1L, 2L, 0.5);
    return result;
  }

  public static class L2APResult extends ForwardingTable<Long, Long, Double> {
    private final HashBasedTable<Long, Long, Double> delegate = HashBasedTable.create();

    @Override
    protected Table<Long, Long, Double> delegate() {
      return delegate;
    }
  }
}