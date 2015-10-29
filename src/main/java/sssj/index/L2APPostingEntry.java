package sssj.index;

public class L2APPostingEntry {
  protected long id;
  protected double weight;
  protected double magnitude;

  public L2APPostingEntry() {
    this(0, 0, 0);
  }

  public L2APPostingEntry(long id, double weight, double magnitude) {
    this.id = id;
    this.weight = weight;
    this.magnitude = magnitude;
  }

  public void setID(long id) {
    this.id = id;
  }

  public void setWeight(double weight) {
    this.weight = weight;
  }

  public void setMagnitude(double magnitude) {
    this.magnitude = magnitude;
  }

  public long id() {
    return id;
  }

  public double weight() {
    return weight;
  }

  public double magnitude() {
    return magnitude;
  }

  @Override
  public String toString() {
    return "[" + id + " -> " + weight + " (" + magnitude + ")]";
  }
}