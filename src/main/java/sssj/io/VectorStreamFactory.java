package sssj.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import sssj.time.Timeline;

public class VectorStreamFactory {
  public static VectorStream getVectorStream(File file, Format format, Timeline timeline) throws FileNotFoundException,
      IOException {
    switch (format) {
    case SSSJ:
    case SVMLIB:
    case VW:
      return new VectorStreamReader(file, format, timeline);
    case BINARY:
      return new BinaryVectorStreamReader(file); // format and timeline have already been applied
    default:
      throw new IllegalArgumentException(String.format("Unsupported format {}", format));
    }
  }
}
