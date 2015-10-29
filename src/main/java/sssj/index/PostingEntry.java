package sssj.index;

public class PostingEntry {
  protected long id;
  protected double weight;

  public PostingEntry() {
    this(0, 0);
  }

  public PostingEntry(long key, double value) {
    this.id = key;
    this.weight = value;
  }

  public void setID(long id) {
    this.id = id;
  }

  public void setWeight(double weight) {
    this.weight = weight;
  }

  public long getID() {
    return id;
  }

  public double getWeight() {
    return weight;
  }

  @Override
  public String toString() {
    return "[" + id + " -> " + weight + "]";
  }
}