package sssj.index.minibatch;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.Iterator;

import sssj.index.PostingEntry;

public class PostingList implements Iterable<PostingEntry> {
  private LongArrayList ids = new LongArrayList();
  private DoubleArrayList weights = new DoubleArrayList();

  public void add(long vectorID, double weight) {
    ids.add(vectorID);
    weights.add(weight);
  }

  public int size() {
    return ids.size();
  }

  @Override
  public String toString() {
    return "[ids=" + ids + ", weights=" + weights + "]";
  }

  @Override
  public Iterator<PostingEntry> iterator() {
    return new Iterator<PostingEntry>() {
      private int i = 0;
      private PostingEntry entry = new PostingEntry();

      @Override
      public boolean hasNext() {
        return i < ids.size();
      }

      @Override
      public PostingEntry next() {
        entry.setID(ids.getLong(i));
        entry.setWeight(weights.getDouble(i));
        i++;
        return entry;
      }

      @Override
      public void remove() {
        i--;
        ids.removeLong(i);
        weights.removeDouble(i);
      }
    };
  }
}