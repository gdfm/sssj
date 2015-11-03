package sssj.index.minibatch.components;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.Iterator;

import sssj.index.L2APPostingEntry;

public class L2APPostingList implements Iterable<L2APPostingEntry> {
  private final LongArrayList ids = new LongArrayList();
  private final DoubleArrayList weights = new DoubleArrayList();
  private final DoubleArrayList magnitudes = new DoubleArrayList();

  public void add(long vectorID, double weight, double magnitude) {
    ids.add(vectorID);
    weights.add(weight);
    magnitudes.add(magnitude);
  }

  public int size() {
    return ids.size();
  }

  @Override
  public String toString() {
    return "[ids=" + ids + ", weights=" + weights + ", magnitudes=" + magnitudes + "]";
  }

  @Override
  public Iterator<L2APPostingEntry> iterator() {
    return new Iterator<L2APPostingEntry>() {
      private final L2APPostingEntry entry = new L2APPostingEntry();
      private int i = 0;

      @Override
      public boolean hasNext() {
        return i < ids.size();
      }

      @Override
      public L2APPostingEntry next() {
        entry.setID(ids.getLong(i));
        entry.setWeight(weights.getDouble(i));
        entry.setMagnitude(magnitudes.getDouble(i));
        i++;
        return entry;
      }

      @Override
      public void remove() {
        i--;
        ids.removeLong(i);
        weights.removeDouble(i);
        magnitudes.removeDouble(i);
      }
    };
  }
}