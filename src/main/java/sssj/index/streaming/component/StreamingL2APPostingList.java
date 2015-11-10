package sssj.index.streaming.component;

import java.util.ListIterator;

import sssj.index.L2APPostingEntry;
import sssj.util.CircularBuffer;

import com.google.common.base.Preconditions;

public class StreamingL2APPostingList implements Iterable<L2APPostingEntry> {
  private final CircularBuffer ids = new CircularBuffer(); // longs
  private final CircularBuffer weights = new CircularBuffer(); // doubles
  private final CircularBuffer magnitudes = new CircularBuffer(); // doubles

  public void add(long vectorID, double weight, double magnitude) {
    ids.pushLong(vectorID);
    weights.pushDouble(weight);
    magnitudes.pushDouble(magnitude);
  }

  public int size() {
    return ids.size();
  }

  @Override
  public String toString() {
    return "[ids=" + ids + ", weights=" + weights + ", magnitudes=" + magnitudes + "]";
  }

  @Override
  public StreamingL2APPostingListIterator iterator() {
    return new StreamingL2APPostingListIterator();
  }

  public StreamingL2APPostingListIterator reverseIterator() {
    return new StreamingL2APPostingListIterator(size());
  }

  public class StreamingL2APPostingListIterator implements ListIterator<L2APPostingEntry> {
    private final L2APPostingEntry entry = new L2APPostingEntry();
    private int i;

    public StreamingL2APPostingListIterator() {
      this(0);
    }

    public StreamingL2APPostingListIterator(int start) {
      Preconditions.checkArgument(i >= 0);
      Preconditions.checkArgument(i <= size());
      this.i = start;
    }

    @Override
    public boolean hasNext() {
      return i < ids.size();
    }

    @Override
    public L2APPostingEntry next() {
      entry.setID(ids.peekLong(i));
      entry.setWeight(weights.peekDouble(i));
      entry.setMagnitude(magnitudes.peekDouble(i));
      i++;
      return entry;
    }

    @Override
    public int nextIndex() {
      return i;
    }

    @Override
    public boolean hasPrevious() {
      return i > 0;
    }

    @Override
    public L2APPostingEntry previous() {
      i--;
      entry.setID(ids.peekLong(i));
      entry.setWeight(weights.peekDouble(i));
      entry.setMagnitude(magnitudes.peekDouble(i));
      return entry;
    }

    @Override
    public int previousIndex() {
      return i - 1;
    }

    @Override
    public void remove() {
      i--;
      assert (i == 0); // removal always happens at the head
      ids.popLong();
      weights.popDouble();
      magnitudes.popDouble();
    }

    public void cutHead() {
      ids.trimHead(i);
      weights.trimHead(i);
      magnitudes.trimHead(i);
      i = 0;
    }

    @Override
    public void set(L2APPostingEntry e) {
      throw new UnsupportedOperationException("Entries in the list are immutable");
    }

    @Override
    public void add(L2APPostingEntry e) {
      throw new UnsupportedOperationException("Entries can only be added at the end of the list");
    }
  }
}