package sssj.index.streaming;

import java.util.Iterator;
import java.util.ListIterator;

import sssj.base.CircularBuffer;
import sssj.index.PostingEntry;

import com.google.common.base.Preconditions;

public class StreamingPostingList implements Iterable<PostingEntry> {
  private CircularBuffer ids = new CircularBuffer(); // longs
  private CircularBuffer weights = new CircularBuffer(); // doubles

  public void add(long vectorID, double weight) {
    ids.pushLong(vectorID);
    weights.pushDouble(weight);
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
    return new StreamingPostingListIterator();
  }

  public StreamingPostingListIterator reverseIterator() {
    return new StreamingPostingListIterator(size());
  }

  class StreamingPostingListIterator implements ListIterator<PostingEntry> {
    private final PostingEntry entry = new PostingEntry();
    private int i = 0;

    public StreamingPostingListIterator() {
      this(0);
    }

    public StreamingPostingListIterator(int start) {
      Preconditions.checkArgument(i >= 0);
      Preconditions.checkArgument(i <= size());
      this.i = start;
    }

    @Override
    public boolean hasNext() {
      return i < ids.size();
    }

    @Override
    public PostingEntry next() {
      entry.setID(ids.peekLong(i));
      entry.setWeight(weights.peekDouble(i));
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
    public PostingEntry previous() {
      i--;
      entry.setID(ids.peekLong(i));
      entry.setWeight(weights.peekDouble(i));
      return entry;
    }

    @Override
    public int previousIndex() {
      return i - 1;
    }

    @Override
    public void remove() {
      i--;
      assert (i == 0); // removals always happen at the head
      ids.popLong();
      weights.popDouble();
    }

    public void cutHead() {
      ids.trimHead(i);
      weights.trimHead(i);
      i = 0;
    }

    @Override
    public void set(PostingEntry e) {
      throw new UnsupportedOperationException("Entries in the list are immutable");
    }

    @Override
    public void add(PostingEntry e) {
      throw new UnsupportedOperationException("Entries can only be added at the end of the list");
    }
  }
}