package sssj.time;

import sssj.Vector;

import com.google.common.base.Function;

public class TimeStamper implements Function<Vector, Vector> {
  private Timeline timeline;

  public TimeStamper(Timeline timeline) {
    this.timeline = timeline;
  }

  @Override
  public Vector apply(Vector input) {
    input.setTimestamp(timeline.nextTimestamp());
    return input;
  }
}
