package sssj.io;

import sssj.Vector;

import com.google.common.base.Function;

public enum Format {
  SSSJ {
    @Override
    public Function<String, Vector> getRecordParser() {
      return new Function<String, Vector>() {
        public Vector apply(String input) {
          String[] tokens = input.split(" ");
          Long ts = Long.parseLong(tokens[0]);
          Vector result = new Vector(ts);
          for (int i = 1; i < tokens.length; i += 2) {
            int key = Integer.parseInt(tokens[i]);
            double val = Double.parseDouble(tokens[i + 1]);
            result.put(key, val);
          }
          return result;
        }
      };
    }
  },

  SVMLIB {
    @Override
    public Function<String, Vector> getRecordParser() {
      return new Function<String, Vector>() {
        public Vector apply(String input) {
          String[] tokens = input.split("\\s");
          // ignore class label tokens[0]
          Vector result = new Vector();
          for (int i = 1; i < tokens.length; i++) {
            String[] parts = tokens[i].split(":");
            int key = Integer.parseInt(parts[0]);
            double val = Double.parseDouble(parts[1]);
            result.put(key, val);
          }
          return result;
        }
      };
    }
  };

  abstract public Function<String, Vector> getRecordParser();
}